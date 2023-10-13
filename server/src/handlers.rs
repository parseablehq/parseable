/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

pub mod http;
pub mod livetail;

const PREFIX_TAGS: &str = "x-p-tag-";
const PREFIX_META: &str = "x-p-meta-";
const STREAM_NAME_HEADER_KEY: &str = "x-p-stream";
const FILL_NULL_OPTION_KEY: &str = "send_null";
const SEPARATOR: char = '^';

const OIDC_SCOPE: &str = "openid profile email";
const COOKIE_AGE_DAYS: usize = 7;
const SESSION_COOKIE_NAME: &str = "session";
const USER_COOKIE_NAME: &str = "username";
