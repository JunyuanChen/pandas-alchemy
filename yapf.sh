#!/bin/sh

TMP_FILE=$(mktemp)
STATUS=0

not_conformant() {
  echo -e "\e[1;35m[ERROR] File $1 does not conform to YAPF style\e[0m"
}

for src_file in pandas_alchemy/*.py; do
  yapf "$src_file" > "$TMP_FILE"
  if ! diff -u --color=always "$src_file" "$TMP_FILE"; then
    not_conformant "$src_file"
    STATUS=1
  fi
done

exit "$STATUS"
