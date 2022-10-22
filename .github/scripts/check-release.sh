#!/bin/bash

# check_tag $current_tag $file_tag $file_name
function check_tag {
  if [[ "$1" != "$2" ]]; then
      echo "Error: the current tag does not match the version in $3: found $2 - expected $1"
      ret=1
  fi
}

ret=0
current_tag=${GITHUB_REF#'refs/tags/v'}

cargo_file='Cargo.toml'
file_tag="$(grep '^version = ' $toml_file | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')"
check_tag $current_tag $file_tag $cargo_file

if [[ "$ret" -eq 0 ]] ; then
  echo 'OK'
fi
exit $ret