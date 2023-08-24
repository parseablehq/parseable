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

use std::sync::Arc;

use actix_web::{
    cookie::{time, Cookie, SameSite},
    http::header,
    web::{self, Data},
    HttpResponse,
};
use anyhow::anyhow;
use log::{error, info};
use openid::{Options, Token, Userinfo};
use serde::Deserialize;
use url::Url;

use crate::{option::CONFIG, utils::uid};

/// Struct representing query params returned from oidc provider
#[derive(Deserialize, Debug)]
pub struct Login {
    pub code: String,
    pub state: Option<String>,
}

/// Struct representing query param when visiting /login
/// Caller can set the state for code auth flow and this is
/// at the end used as target for redirect
#[derive(Deserialize, Debug)]
pub struct RedirectAfterLogin {
    pub redirect: Url,
}

pub async fn login(
    query: web::Query<RedirectAfterLogin>,
    oidc_client: Data<openid::Client>,
) -> HttpResponse {
    let redirect = query.into_inner().redirect.to_string();
    let auth_url = oidc_client.auth_url(&Options {
        scope: Some("openid email".into()),
        state: Some(redirect),
        ..Default::default()
    });
    info!("authorize: {}", auth_url);
    let url: String = auth_url.into();
    HttpResponse::TemporaryRedirect()
        .insert_header((header::LOCATION, url))
        .finish()
}

/// Handler for code callback
/// User should be redirected to page they were trying to access with cookie
pub async fn reply_login(
    oidc_client: Data<openid::Client>,
    login_query: web::Query<Login>,
) -> HttpResponse {
    let oidc_client = Data::into_inner(oidc_client);
    let request_token = request_token(oidc_client, &login_query).await;
    match request_token {
        Ok((_token, _user_info)) => {
            let id = uid::gen().to_string();

            // let login = user_info.preferred_username.clone();
            // let email = user_info.email.clone();

            // let user = User {
            //     id: user_info.sub.clone().unwrap_or_default(),
            //     login,
            //     last_name: user_info.family_name.clone(),
            //     first_name: user_info.name.clone(),
            //     email,
            //     activated: user_info.email_verified,
            //     image_url: user_info.picture.clone().map(|x| x.to_string()),
            //     lang_key: Some("en".to_string()),
            //     authorities: vec!["ROLE_USER".to_string()],
            // };

            let authorization_cookie = Cookie::build("session", id)
                .http_only(true)
                .max_age(time::Duration::days(1))
                .same_site(SameSite::Strict)
                .path("/")
                .finish();

            // sessions
            //     .write()
            //     .await
            //     .map
            //     .insert(id, (user, token, user_info));

            let redirect_url = login_query
                .state
                .clone()
                .unwrap_or_else(|| CONFIG.parseable.address.to_string());

            HttpResponse::MovedPermanently()
                .insert_header((header::LOCATION, redirect_url))
                .cookie(authorization_cookie)
                .body("")
        }
        Err(err) => {
            error!("login error in call: {:?}", err);
            HttpResponse::Unauthorized().finish()
        }
    }
}

async fn request_token(
    oidc_client: Arc<openid::Client>,
    login_query: &Login,
) -> anyhow::Result<(Token, Userinfo)> {
    let mut token: Token = oidc_client.request_token(&login_query.code).await?.into();
    if let Some(id_token) = token.id_token.as_mut() {
        oidc_client.decode_token(id_token)?;
        oidc_client.validate_token(id_token, None, None)?;
        info!("token: {:?}", id_token);
    } else {
        return Err(anyhow!("No id_token provided"));
    }
    let userinfo = oidc_client.request_userinfo(&token).await?;
    info!("user info: {:?}", userinfo);
    Ok((token, userinfo))
}
