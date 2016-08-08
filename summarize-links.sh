#!/bin/bash
grep -h "linkReason" "$@"|sort|uniq -c|sort -h
