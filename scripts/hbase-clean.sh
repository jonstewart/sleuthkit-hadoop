#!/bin/bash

echo "disable 'entries'; disable 'hash'; disable 'images'; drop 'entries'; drop 'hash'; drop 'images'" | hbase shell
