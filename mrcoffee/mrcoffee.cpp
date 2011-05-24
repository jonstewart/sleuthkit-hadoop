
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <vector>

#include <unistd.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <boost/scoped_array.hpp>

#include "io.h"

/*
void closer(int* sock) {
  // Always Be Closing
  if (close(*sock) == -1) {
    THROW("close: " << strerror(errno));
  }
}
*/

int exec_cmd(char** argv, int* in_pipe, int* out_pipe, int* err_pipe) {
  // fork the child
  const int pid = fork();
  if (pid == -1) {
    THROW("fork: " << strerror(errno));
  }

  if (pid != 0) {
    // we are the parent
    return pid;
  }

  // close the pipe ends we don't use
  if (in_pipe[1] != -1) {
    CLOSE(in_pipe[1]);
  }

  CLOSE(out_pipe[0]);
  CLOSE(err_pipe[0]);

  // read child's stdin from parent
  if (in_pipe[0] != -1) {
    DUP2(in_pipe[0], STDIN_FILENO);
    CLOSE(in_pipe[0]);
  }

  // send child's stdout to parent
  DUP2(out_pipe[1], STDOUT_FILENO);
  CLOSE(out_pipe[1]);

  // send child's stderr to parent
  DUP2(err_pipe[1], STDERR_FILENO);
  CLOSE(err_pipe[1]);

  // run the command
  if (execvp(argv[0], argv) == -1) {
    THROW("execvp: " << strerror(errno));
  }

  THROW("wtf: execvp returned something other than -1");
}

bool handle_client_request(int c_sock) {
  ssize_t rlen, wlen;
  size_t len, off;

// FIXME: inefficient, maybe make this a class member?
  char buf[4096];

  // read command length from client
  off = 0;
  while (off < sizeof(len)) {
    off += rlen = read(c_sock, &len+off, sizeof(len)-off);
    if (rlen == -1) {
      THROW("read: " << strerror(errno));
    }
    else if (rlen == 0) {
      // client has disconnected
      return false;
    }
  }

  // read command line from client
  boost::scoped_array<char> cmdline(new char[len]);
  read_bytes(c_sock, cmdline.get(), len);

  // break command line into command and arguments
  std::vector<char*> args;
  args.push_back(cmdline.get());
  for (unsigned int i = 0; i < len-1; ++i) {
    if (cmdline[i] == '\0') {
      args.push_back(cmdline.get()+i+1);
    }
  } 
  
  args.push_back(NULL); // last arg for execvp must be NULL

  // read data length from client
  size_t in_len;
  read_bytes(c_sock, (char*) &in_len, sizeof(in_len));

  // set up the pipes; pipes named from the POV of the child
  int in_pipe[2], out_pipe[2], err_pipe[2];

  if (in_len > 0) {
    // create in pipe only if the client will send data
    PIPE(in_pipe);
  }
  else {
    in_pipe[0] = in_pipe[1] = -1;
  }

  PIPE(out_pipe);
  PIPE(err_pipe);

  // fork the child to run the command
  const int ch_pid = exec_cmd(args.data(), in_pipe, out_pipe, err_pipe);

  // close the pipe ends we don't use
  CLOSE(out_pipe[1]);
  CLOSE(err_pipe[1]);

  if (in_pipe[0] != -1) {
    CLOSE(in_pipe[0]);
  }

  std::vector<char> ch_out, ch_err;

  // read from client socket, child stdout, stderr and write to child
  // stdin as data is available
  fd_set rfds, wfds;
  int nfds;

  char in_buf[4096];
  size_t in_available = 0, in_written = 0;

  do {
    // create the set of file descriptors on which to select
    nfds = 0;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);

    if (out_pipe[0] != -1) {
      FD_SET(out_pipe[0], &rfds);
      nfds = std::max(nfds, out_pipe[0]);
    }

    if (err_pipe[0] != -1) {
      FD_SET(err_pipe[0], &rfds);
      nfds = std::max(nfds, err_pipe[0]);
    }

    if (in_len > 0) {
      FD_SET(c_sock, &rfds);
      nfds = std::max(nfds, c_sock);
    }

    if (in_pipe[1] != -1) {
      FD_SET(in_pipe[1], &wfds);
      nfds = std::max(nfds, in_pipe[1]);
    }

    // determine whether any file descriptors are ready
    if (select(nfds+1, &rfds, &wfds, NULL, NULL) == -1) {
      THROW("select: " << strerror(errno));
    }

    // read data from client socket
    if (FD_ISSET(c_sock, &rfds)) {
      // read from client socket
      rlen = read(c_sock, in_buf+in_available,
                  std::min(sizeof(in_buf)-in_available, in_len));
      if (rlen == -1) {
        THROW("read: " << strerror(errno));
      }
      else if (rlen == 0) {
        THROW("read: client shut down socket"); 
      }
      else {
        in_len -= rlen;
        in_available += rlen;
      }
    }

    // write data to child stdin        
    if (FD_ISSET(in_pipe[1], &wfds)) {
      wlen = write(in_pipe[1], in_buf+in_written,
                   in_available-in_written);
      if (wlen == -1) {
        THROW("write: " << strerror(errno));
      }
      else {
        in_written += wlen;

        if (in_written == in_available) {
          // output has caught up with input
          in_written = in_available = 0;

          if (in_len == 0) {
            // close child's stdin
            CLOSE(in_pipe[1]);
            in_pipe[1] = -1;
          }
        }
      }
    }

    // collect data from child stdout
    if (FD_ISSET(out_pipe[0], &rfds)) {
      // read from child stdout
      rlen = read(out_pipe[0], buf, sizeof(buf));
      if (rlen == -1) {
        THROW("read: " << strerror(errno));
      }
      else if (rlen == 0) {
        // child closed stdout
        CLOSE(out_pipe[0]);
        out_pipe[0] = -1;
      }
      else {
        // store child's stdout in ch_out
        ch_out.insert(ch_out.end(), buf, buf+rlen);
      }
    }

    // collect data from child stderr
    if (FD_ISSET(err_pipe[0], &rfds)) {
      // read from child stdout
      rlen = read(err_pipe[0], buf, sizeof(buf));
      if (rlen == -1) {
        THROW("read: " << strerror(errno));
      }
      else if (rlen == 0) {
        // child closed stderr
        CLOSE(err_pipe[0]);
        err_pipe[0] = -1;
      }
      else {
        // store child stderr in ch_err
        ch_err.insert(ch_err.end(), buf, buf+rlen);
      }
    }
  } while (in_pipe[1] != -1 || out_pipe[0] != -1 || err_pipe[0] != -1);

  // wait for the child to exit
  if (waitpid(ch_pid, NULL, 0) == -1) {
    THROW("waitpid: " << strerror(errno));
  }

  // send child's stdout to the client
  len = ch_out.size();
  write_bytes(c_sock, (char*) &len, sizeof(len));
  write_bytes(c_sock, ch_out.data(), len);

  // send child's stderr to the client
  len = ch_err.size();
  write_bytes(c_sock, (char*) &len, sizeof(len));
  write_bytes(c_sock, ch_err.data(), len);

  return true;
}

int main(int argc, char** argv) {
  try {
    // get the socket
    int s_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (s_sock == -1) {
      THROW("socket: " << strerror(errno));
    }

    // bind to all interfaces 
    in_addr ipaddr;
    ipaddr.s_addr = INADDR_ANY;

    sockaddr_in s_addr;
    memset(&s_addr, 0, sizeof(s_addr));
    s_addr.sin_family = AF_INET;
    s_addr.sin_port   = 31337;
    s_addr.sin_addr   = ipaddr;

    if (bind(s_sock, (sockaddr*) &s_addr, sizeof(s_addr)) == -1) {
      THROW("bind: " << strerror(errno));
    }

    while (1) {
      // listen on our socket
      if (listen(s_sock, 10) == -1) {
        THROW("listen: " << strerror(errno));
      }

      // accept connection from client
      sockaddr_in c_addr;
      memset(&c_addr, 0, sizeof(c_addr));
      socklen_t c_addr_len = sizeof(c_addr);

      int c_sock = accept(s_sock, (sockaddr*) &c_addr, &c_addr_len);
      if (c_sock == -1) {
        THROW("accept: " << strerror(errno));
      } 

      // handle client requests
      try {
        while (handle_client_request(c_sock));
      }
      catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
      }

      // close the client socket 
      CLOSE(c_sock);
    }

    // close the server socket
    if (close(s_sock) == -1) {
      THROW("close: " << strerror(errno));
    }
  }
  catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}
