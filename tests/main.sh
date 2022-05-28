#!/bin/sh
#
# Parseable Server (C) 2022 Parseable, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

mode=$1
endpoint=$2

run_smoke_test () {
  echo "Executing smoke test"
  #stream_name=$(echo $RANDOM | md5sum | head -c 20)
  #./testcases/smoke_test.sh "$endpoint" "$stream_name"
  ./testcases/smoke_test.sh "$endpoint" "testapistream02"
  return $?
}

case "$mode" in
   "smoke") run_smoke_test 
   ;;
esac
