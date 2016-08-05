#!/bin/bash
grep "linkReason" "$1"|sort|uniq -c|sort -h
