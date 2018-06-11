{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE NamedFieldPuns    #-}

module Brig.API (runServer, parseOptions) where

import Brig.App
import Brig.AWS (sesQueue)
import Brig.API.Error
import Brig.API.Handler
import Brig.API.Types
import Brig.Options hiding (sesQueue)
import Brig.Types
import Brig.Types.Intra
import Brig.Types.User (NewUserNoSSO(NewUserNoSSO))
import Brig.Types.User.Auth
import Brig.User.Email
import Brig.User.Phone
import Control.Error
import Control.Lens (view, (^.))
import Control.Monad (liftM2, liftM3, unless, void, when)
import Control.Monad.Catch (finally)
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Data.Aeson hiding (json)
import Data.ByteString.Conversion
import Data.Foldable (for_)
import Data.Id
import Data.Int
import Data.Metrics.Middleware hiding (metrics)
import Data.Misc (IpAddr (..))
import Data.Monoid ((<>))
import Data.Range
import Data.Text (Text, unpack)
import Data.Text.Encoding (decodeLatin1)
import Data.Text.Lazy (pack)
import Galley.Types (UserClients (..))
import Network.HTTP.Types.Status
import Network.Wai (Request, Response, responseLBS, lazyRequestBody)
import Network.Wai.Predicate hiding (setStatus, result)
import Network.Wai.Routing
import Network.Wai.Utilities hiding (parseJsonBody)
import Network.Wai.Utilities.Server
import Network.Wai.Utilities.Swagger (document, mkSwaggerApi)
import Prelude hiding (head)
import Util.Options

import qualified Data.Text.Ascii               as Ascii
import qualified Data.List1                    as List1
import qualified Control.Concurrent.Async      as Async
import qualified Brig.API.Client               as API
import qualified Brig.API.Connection           as API
import qualified Brig.API.Properties           as API
import qualified Brig.API.User                 as API
import qualified Brig.Team.Util                as Team
import qualified Brig.User.API.Auth            as Auth
import qualified Brig.User.API.Search          as Search
import qualified Brig.User.Auth.Cookie         as Auth
import qualified Brig.AWS                      as AWS
import qualified Brig.AWS.SesNotification      as SesNotification
import qualified Brig.InternalEvent.Process    as Internal
import qualified Brig.Types.Swagger            as Doc
import qualified Network.Wai.Utilities.Swagger as Doc
import qualified Data.Swagger.Build.Api        as Doc
import qualified Galley.Types.Swagger          as Doc
import qualified Galley.Types.Teams            as Team
import qualified Network.Wai.Middleware.Gzip   as GZip
import qualified Network.Wai.Middleware.Gunzip as GZip
import qualified Network.Wai.Utilities         as Utilities
import qualified Data.ByteString.Lazy          as Lazy
import qualified Data.Map.Strict               as Map
import qualified Network.Wai.Utilities.Server  as Server
import qualified Data.Set                      as Set
import qualified Data.Text                     as Text
import qualified Brig.Provider.API             as Provider
import qualified Brig.Team.API                 as Team
import qualified Brig.Team.Email               as Team
import qualified Brig.TURN.API                 as TURN
import qualified System.Logger.Class           as Log

runServer :: Opts -> IO ()
runServer o = do
    e <- newEnv o
    s <- Server.newSettings (server e)
    emailListener <- case (e^.awsEnv.sesQueue) of
            Just q -> Just <$> listen (e^.awsEnv) e q SesNotification.onEvent
            _      -> return Nothing
    internalEventListener <- Async.async $ Internal.listen e
    runSettingsWithShutdown s (pipeline e) 5 `finally` do
        mapM_ Async.cancel emailListener
        Async.cancel internalEventListener
        closeEnv e
  where
    rtree      = compile (sitemap o)
    endpoint   = brig o
    server   e = defaultServer (unpack $ endpoint^.epHost) (endpoint^.epPort) (e^.applog) (e^.metrics)
    pipeline e = measureRequests (e^.metrics) rtree
               . catchErrors (e^.applog) (e^.metrics)
               . GZip.gunzip . GZip.gzip GZip.def
               $ serve e

    serve e r k = runHandler e r (Server.route rtree r k) k

    listen aws env queue f = Async.async $ AWS.execute aws $ AWS.listen queue (runAppT env . f)

---------------------------------------------------------------------------
-- Sitemap

sitemap :: Opts -> Routes Doc.ApiBuilder Handler ()
sitemap o = do

    -- Internal ---------------------------------------------------------------

    get  "/i/status" (continue $ const $ return empty) true
    head "/i/status" (continue $ const $ return empty) true

    get "/i/monitoring" (continue $ const $ view metrics >>= fmap json . render) $
        accept "application" "json"

    post "/i/users/:id/auto-connect" (continue autoConnect) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. capture "id"
        .&. opt (header "Z-Connection")
        .&. request

    post "/i/users" (continue createUserNoVerify) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. request

    put "/i/self/email" (continue changeSelfEmailNoSend) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. request

    delete "/i/users/:id" (continue deleteUserNoVerify) $
        capture "id"

    get "/i/users/connections-status" (continue deprecatedGetConnectionsStatus) $
        query "users"
        .&. opt (query "filter")

    post "/i/users/connections-status" (continue getConnectionsStatus) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. request
        .&. opt (query "filter")

    get "/i/users" (continue listActivatedAccounts) $
        accept "application" "json"
        .&. (param "ids" ||| param "handles")

    get "/i/users" (continue listAccountsByIdentity) $
        accept "application" "json"
        .&. (param "email" ||| param "phone")

    put "/i/users/:id/status" (continue changeAccountStatus) $
        contentType "application" "json"
        .&. capture "id"
        .&. request

    get "/i/users/:id/status" (continue getAccountStatus) $
        accept "application" "json"
        .&. capture "id"

    get "/i/users/:id/contacts" (continue getContactList) $
        accept "application" "json"
        .&. capture "id"

    get "/i/users/activation-code" (continue getActivationCode) $
        accept "application" "json"
        .&. (param "email" ||| param "phone")

    get "/i/users/password-reset-code" (continue getPasswordResetCode) $
        accept "application" "json"
        .&. (param "email" ||| param "phone")

    post "/i/users/revoke-identity" (continue revokeIdentity) $
        param "email" ||| param "phone"

    head "/i/users/blacklist" (continue checkBlacklist) $
        param "email" ||| param "phone"

    delete "/i/users/blacklist" (continue deleteFromBlacklist) $
        param "email" ||| param "phone"

    post "/i/users/blacklist" (continue addBlacklist) $
        param "email" ||| param "phone"

    -- is :uid not team owner, or there are other team owners?
    get "/i/users/:uid/can-be-deleted/:tid" (continue canBeDeleted) $
      capture "uid"
      .&. capture "tid"

    -- is :uid team owner (the only one or one of several)?
    get "/i/users/:uid/is-team-owner/:tid" (continue isTeamOwner) $
      capture "uid"
      .&. capture "tid"

    post "/i/clients" (continue internalListClients) $
      accept "application" "json"
      .&. contentType "application" "json"
      .&. request

    -- /users -----------------------------------------------------------------

    get "/users/api-docs"
        (\(_ ::: url) k ->
            let doc = encode $ mkSwaggerApi (decodeLatin1 url) Doc.brigModels (sitemap o)
            in k $ responseLBS status200 [jsonContent] doc) $
        accept "application" "json"
        .&. query "base_url"

    ---

    head "/users/:id" (continue checkUserExists) $
        header "Z-User"
        .&. capture "id"

    document "HEAD" "userExists" $ do
        Doc.summary "Check if a user ID exists"
        Doc.parameter Doc.Path "id" Doc.bytes' $
            Doc.description "User ID"
        Doc.response 200 "User exists" Doc.end
        Doc.errorResponse userNotFound

    ---

    get "/users/:id" (continue getUser) $
        accept "application" "json"
        .&. header "Z-User"
        .&. capture "id"

    document "GET" "user" $ do
        Doc.summary "Get a user by ID"
        Doc.parameter Doc.Path "id" Doc.bytes' $
            Doc.description "User ID"
        Doc.returns (Doc.ref Doc.user)
        Doc.response 200 "User" Doc.end
        Doc.errorResponse userNotFound

    ---

    post "/users/handles" (continue checkHandles) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. header "Z-User"
        .&. request

    document "POST" "checkUserHandles" $ do
        Doc.summary "Check availability of user handles"
        Doc.body (Doc.ref Doc.checkHandles) $
            Doc.description "JSON body"
        Doc.returns (Doc.array Doc.string')
        Doc.response 200 "List of free handles" Doc.end

    head "/users/handles/:handle" (continue checkHandle) $
        header "Z-User"
        .&. capture "handle"

    document "HEAD" "checkUserHandle" $ do
        Doc.summary "Check whether a user handle can be taken"
        Doc.parameter Doc.Path "handle" Doc.bytes' $
            Doc.description "Handle to check"
        Doc.response 200 "Handle is taken" Doc.end
        Doc.errorResponse invalidHandle
        Doc.errorResponse handleNotFound

    get "/users/handles/:handle" (continue getHandleInfo) $
        accept "application" "json"
        .&. header "Z-User"
        .&. capture "handle"

    document "GET" "getUserHandleInfo" $ do
        Doc.summary "Get information on a user handle"
        Doc.parameter Doc.Path "handle" Doc.bytes' $
            Doc.description "The user handle"
        Doc.returns (Doc.ref Doc.userHandleInfo)
        Doc.response 200 "Handle info" Doc.end
        Doc.errorResponse handleNotFound

    ---

    get "/users" (continue listUsers) $
        accept "application" "json"
        .&. header "Z-User"
        .&. (param "ids" ||| param "handles")

    document "GET" "users" $ do
        Doc.summary "List users"
        Doc.notes "The 'ids' and 'handles' parameters are mutually exclusive."
        Doc.parameter Doc.Query "ids" Doc.string' $ do
            Doc.description "User IDs of users to fetch"
            Doc.optional
        Doc.parameter Doc.Query "handles" Doc.string' $ do
            Doc.description "Handles of users to fetch"
            Doc.optional
        Doc.returns (Doc.array (Doc.ref Doc.user))
        Doc.response 200 "List of users" Doc.end

    ---

    post "/users/prekeys" (continue getMultiPrekeyBundles) $
        request
        .&. contentType "application" "json"
        .&. accept "application" "json"

    document "POST" "getMultiPrekeyBundles" $ do
        Doc.summary "Given a map of user IDs to client IDs return a \
        \prekey for each one. The maximum map size is 128 entries."
        Doc.notes "Prekeys of all clients of a multiple users. \
        \The result is a map of maps, i.e. { UserId : { ClientId : Maybe Prekey } }"
        Doc.body (Doc.ref Doc.userClients) $
            Doc.description "JSON body"
        Doc.response 200 "Prekey Bundles" Doc.end
        Doc.errorResponse tooManyClients

    ---

    get "/users/:user/prekeys" (continue getPrekeyBundle) $
        capture "user"
        .&. accept "application" "json"

    document "GET" "getPrekeyBundle" $ do
        Doc.summary "Get a prekey for each client of a user."
        Doc.parameter Doc.Path "user" Doc.bytes' $
            Doc.description "User ID"
        Doc.returns (Doc.ref Doc.prekeyBundle)
        Doc.response 200 "Prekey Bundle" Doc.end

    ---

    get "/users/:user/prekeys/:client" (continue getPrekey) $
        capture "user"
        .&. capture "client"
        .&. accept "application" "json"

    document "GET" "getPrekey" $ do
        Doc.summary "Get a prekey for a specific client of a user."
        Doc.parameter Doc.Path "user" Doc.bytes' $
            Doc.description "User ID"
        Doc.parameter Doc.Path "client" Doc.bytes' $
            Doc.description "Client ID"
        Doc.returns (Doc.ref Doc.clientPrekey)
        Doc.response 200 "Client Prekey" Doc.end

    --

    get "/users/:user/clients" (continue getUserClients) $
        capture "user"
        .&. accept "application" "json"

    document "GET" "getUserClients" $ do
        Doc.summary "Get all of a user's clients."
        Doc.parameter Doc.Path "user" Doc.bytes' $
            Doc.description "User ID"
        Doc.returns (Doc.array (Doc.ref Doc.pubClient))
        Doc.response 200 "List of clients" Doc.end

    --

    get "/users/:user/clients/:client" (continue getUserClient) $
        capture "user"
        .&. capture "client"
        .&. accept "application" "json"

    document "GET" "getUserClient" $ do
        Doc.summary "Get a specific client of a user."
        Doc.parameter Doc.Path "user" Doc.bytes' $
            Doc.description "User ID"
        Doc.parameter Doc.Path "client" Doc.bytes' $
            Doc.description "Client ID"
        Doc.returns (Doc.ref Doc.pubClient)
        Doc.response 200 "Client" Doc.end

    -- /self ------------------------------------------------------------------

    -- Profile

    get "/self" (continue getSelf) $
        accept "application" "json"
        .&. header "Z-User"

    document "GET" "self" $ do
        Doc.summary "Get your profile"
        Doc.returns (Doc.ref Doc.self)
        Doc.response 200 "Self profile" Doc.end

    ---

    put "/self" (continue updateUser) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. request

    document "PUT" "updateSelf" $ do
        Doc.summary "Update your profile"
        Doc.body (Doc.ref Doc.userUpdate) $
            Doc.description "JSON body"
        Doc.response 200 "Update successful." Doc.end

    ---

    get "/self/name" (continue getUserName) $
        accept "application" "json"
        .&. header "Z-User"

    document "GET" "selfName" $ do
        Doc.summary "Get your profile name"
        Doc.returns (Doc.ref Doc.userName)
        Doc.response 200 "Profile name found." Doc.end

    ---

    put "/self/email" (continue changeSelfEmail) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. request

    document "PUT" "changeEmail" $ do
        Doc.summary "Change your email address"
        Doc.body (Doc.ref Doc.emailUpdate) $
            Doc.description "JSON body"
        Doc.response 202 "Update accepted and pending activation of the new email." Doc.end
        Doc.response 204 "No update, current and new email address are the same." Doc.end
        Doc.errorResponse invalidEmail
        Doc.errorResponse userKeyExists
        Doc.errorResponse blacklistedEmail
        Doc.errorResponse blacklistedPhone

    ---

    put "/self/phone" (continue changePhone) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. request

    document "PUT" "changePhone" $ do
        Doc.summary "Change your phone number"
        Doc.body (Doc.ref Doc.phoneUpdate) $
            Doc.description "JSON body"
        Doc.response 202 "Update accepted and pending activation of the new phone number." Doc.end
        Doc.errorResponse userKeyExists

    ---

    head "/self/password" (continue checkPasswordExists) $
        header "Z-User"

    document "HEAD" "checkPassword" $ do
        Doc.summary "Check that your passowrd is set"
        Doc.response 200 "Password is set." Doc.end
        Doc.response 404 "Password is not set." Doc.end

    ---

    put "/self/password" (continue changePassword) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. request

    document "PUT" "changePassword" $ do
        Doc.summary "Change your password"
        Doc.body (Doc.ref Doc.changePassword) $
            Doc.description "JSON body"
        Doc.response 200 "Password changed." Doc.end
        Doc.errorResponse badCredentials
        Doc.errorResponse noIdentity

    --

    put "/self/locale" (continue changeLocale) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. request

    document "PUT" "changeLocale" $ do
        Doc.summary "Change your locale"
        Doc.body (Doc.ref Doc.changeLocale) $
            Doc.description "JSON body"
        Doc.response 200 "Locale changed." Doc.end

    --

    put "/self/handle" (continue changeHandle) $
        contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. request

    document "PUT" "changeHandle" $ do
        Doc.summary "Change your handle"
        Doc.body (Doc.ref Doc.changeHandle) $
            Doc.description "JSON body"
        Doc.errorResponse handleExists
        Doc.errorResponse invalidHandle
        Doc.response 200 "Handle changed." Doc.end

    ---

    delete "/self/phone" (continue removePhone) $
        header "Z-User"
        .&. header "Z-Connection"

    document "DELETE" "removePhone" $ do
        Doc.summary "Remove your phone number."
        Doc.notes "Your phone number can only be removed if you also have an \
                  \email address and a password."
        Doc.response 200 "Phone number removed." Doc.end
        Doc.errorResponse lastIdentity
        Doc.errorResponse noPassword

    ---

    delete "/self/email" (continue removeEmail) $
        header "Z-User"
        .&. header "Z-Connection"

    document "DELETE" "removeEmail" $ do
        Doc.summary "Remove your email address."
        Doc.notes "Your email address can only be removed if you also have a \
                  \phone number."
        Doc.response 200 "Email address removed." Doc.end
        Doc.errorResponse lastIdentity


    ---
    ---

    delete "/self" (continue deleteUser) $
        header "Z-User"
        .&. request
        .&. contentType "application" "json"
        .&. accept "application" "json"

    document "DELETE" "deleteUser" $ do
        Doc.summary "Initiate account deletion."
        Doc.notes "If the account has a verified identity, a verification \
                  \code is sent and needs to be confirmed to authorise the \
                  \deletion. If the account has no verified identity but a \
                  \password, it must be provided. If password is correct, or if neither \
                  \a verified identity nor a password exists, account deletion \
                  \is scheduled immediately."
        Doc.body (Doc.ref Doc.delete) $
            Doc.description "JSON body"
        Doc.response 202 "Deletion is pending verification with a code." Doc.end
        Doc.response 200 "Deletion is initiated." Doc.end
        Doc.errorResponse badCredentials
        Doc.errorResponse missingAuthError

    ---

    post "/delete" (continue verifyDeleteUser) $
        request
        .&. contentType "application" "json"
        .&. accept "application" "json"

    document "POST" "verifyDeleteUser" $ do
        Doc.summary "Verify account deletion with a code."
        Doc.body (Doc.ref Doc.verifyDelete) $
            Doc.description "JSON body"
        Doc.response 200 "Deletion is initiated." Doc.end
        Doc.errorResponse invalidCode

    ---

    post "/connections" (continue createConnection) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. request

    document "POST" "createConnection" $ do
        Doc.summary "Create a connection to another user."
        Doc.notes $ "You can have no more than "
                 <> Text.pack (show (setUserMaxConnections $ optSettings o))
                 <> " connections in accepted or sent state."
        Doc.body (Doc.ref Doc.connectionRequest) $
            Doc.description "JSON body"
        Doc.returns (Doc.ref Doc.connection)
        Doc.response 200 "The connection exists." Doc.end
        Doc.response 201 "The connection was created." Doc.end
        Doc.errorResponse connectionLimitReached
        Doc.errorResponse invalidUser
        Doc.errorResponse noIdentity

    ---

    get "/connections" (continue listConnections) $
        accept "application" "json"
        .&. header "Z-User"
        .&. opt (query "start")
        .&. def (unsafeRange 100) (query "size")

    document "GET" "connections" $ do
        Doc.summary "List the connections to other users."
        Doc.parameter Doc.Query "start" Doc.string' $ do
            Doc.description "User ID to start from"
            Doc.optional
        Doc.parameter Doc.Query "size" Doc.int32' $ do
            Doc.description "Number of results to return (default 100, max 500)."
            Doc.optional
        Doc.returns (Doc.ref Doc.connectionList)
        Doc.response 200 "List of connections" Doc.end

    ---

    put "/connections/:id" (continue updateConnection) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. capture "id"
        .&. request

    document "PUT" "updateConnection" $ do
        Doc.summary "Update a connection."
        Doc.parameter Doc.Path "id" Doc.bytes' $
            Doc.description "User ID"
        Doc.body (Doc.ref Doc.connectionUpdate) $
            Doc.description "JSON body"
        Doc.returns (Doc.ref Doc.connection)
        Doc.response 200 "Connection updated." Doc.end
        Doc.response 204 "No change." Doc.end
        Doc.errorResponse connectionLimitReached
        Doc.errorResponse invalidTransition
        Doc.errorResponse notConnected
        Doc.errorResponse invalidUser

    ---

    get "/connections/:id" (continue getConnection) $
        accept "application" "json"
        .&. header "Z-User"
        .&. capture "id"

    document "GET" "connection" $ do
        Doc.summary "Get an existing connection to another user."
        Doc.parameter Doc.Path "id" Doc.bytes' $
            Doc.description "User ID"
        Doc.returns (Doc.ref Doc.connection)
        Doc.response 200 "Connection" Doc.end

    --- Clients

    post "/clients" (continue addClient) $
        request
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. opt (header "X-Forwarded-For")
        .&. accept "application" "json"
        .&. contentType "application" "json"

    document "POST" "registerClient" $ do
        Doc.summary "Register a new client."
        Doc.body (Doc.ref Doc.newClient) $
            Doc.description "JSON body"
        Doc.returns (Doc.ref Doc.client)
        Doc.response 200 "Client" Doc.end
        Doc.errorResponse tooManyClients
        Doc.errorResponse missingAuthError
        Doc.errorResponse malformedPrekeys

    ---

    put "/clients/:client" (continue updateClient) $
        request
        .&. header "Z-User"
        .&. capture "client"
        .&. accept "application" "json"
        .&. contentType "application" "json"

    document "PUT" "updateClient" $ do
        Doc.summary "Update a registered client."
        Doc.parameter Doc.Path "client" Doc.bytes' $
            Doc.description "Client ID"
        Doc.body (Doc.ref Doc.updateClient) $
            Doc.description "JSON body"
        Doc.response 200 "Client updated." Doc.end
        Doc.errorResponse malformedPrekeys

    ---

    delete "/clients/:client" (continue rmClient) $
        request
        .&. header "Z-User"
        .&. header "Z-Connection"
        .&. capture "client"
        .&. accept "application" "json"
        .&. contentType "application" "json"

    document "DELETE" "deleteClient" $ do
        Doc.summary "Delete an existing client."
        Doc.parameter Doc.Path "client" Doc.bytes' $
            Doc.description "Client ID"
        Doc.body (Doc.ref Doc.deleteClient) $
            Doc.description "JSON body"
        Doc.response 200 "Client deleted." Doc.end

    ---

    get "/clients" (continue listClients) $
        header "Z-User"
        .&. accept "application" "json"

    document "GET" "listClients" $ do
        Doc.summary "List the registered clients."
        Doc.returns (Doc.array (Doc.ref Doc.client))
        Doc.response 200 "List of clients" Doc.end

    ---

    get "/clients/:client" (continue getClient) $
        header "Z-User"
        .&. capture "client"
        .&. accept "application" "json"

    document "GET" "getClients" $ do
        Doc.summary "Get a registered client by ID."
        Doc.parameter Doc.Path "client" Doc.bytes' $
            Doc.description "Client ID"
        Doc.returns (Doc.ref Doc.client)
        Doc.response 200 "Client" Doc.end

    ---

    get "/clients/:client/prekeys" (continue listPrekeyIds) $
        header "Z-User"
        .&. capture "client"
        .&. accept "application" "json"

    document "GET" "listPrekeyIds" $ do
        Doc.summary "List the remaining prekey IDs of a client."
        Doc.parameter Doc.Path "client" Doc.bytes' $
            Doc.description "Client ID"
        Doc.returns (Doc.array Doc.string')
        Doc.response 200 "List of remaining prekey IDs." Doc.end

    --- Properties

    put "/properties/:key" (continue setProperty) $
        header "Z-User"
        .&. header "Z-Connection"
        .&. capture "key"
        .&. request
        .&. contentType "application" "json"

    document "PUT" "setProperty" $ do
        Doc.summary "Set a user property."
        Doc.parameter Doc.Path "key" Doc.string' $
            Doc.description "Property key"
        Doc.body (Doc.ref Doc.propertyValue) $
            Doc.description "JSON body"
        Doc.response 200 "Property set." Doc.end

    ---

    delete "/properties/:key" (continue deleteProperty) $
        header "Z-User"
        .&. header "Z-Connection"
        .&. capture "key"

    document "DELETE" "deleteProperty" $ do
        Doc.summary "Delete a property."
        Doc.parameter Doc.Path "key" Doc.string' $
            Doc.description "Property key"
        Doc.response 200 "Property deleted." Doc.end

    ---

    delete "/properties" (continue clearProperties) $
        header "Z-User"
        .&. header "Z-Connection"

    document "DELETE" "clearProperties" $ do
        Doc.summary "Clear all properties."
        Doc.response 200 "Properties cleared." Doc.end

    ---

    get "/properties/:key" (continue getProperty) $
        header "Z-User"
        .&. capture "key"
        .&. accept "application" "json"

    document "GET" "getProperty" $ do
        Doc.summary "Get a property value."
        Doc.parameter Doc.Path "key" Doc.string' $
            Doc.description "Property key"
        Doc.returns (Doc.ref Doc.propertyValue)
        Doc.response 200 "The property value." Doc.end

    ---

    get "/properties" (continue listPropertyKeys) $
        header "Z-User"
        .&. accept "application" "json"

    document "GET" "listPropertyKeys" $ do
        Doc.summary "List all property keys."
        Doc.returns (Doc.array Doc.string')
        Doc.response 200 "List of property keys." Doc.end

    -- /register, /activate, /password-reset ----------------------------------

    post "/register" (continue createUser) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. request

    document "POST" "register" $ do
        Doc.summary "Register a new user."
        Doc.notes   "If the environment where the registration takes \
                    \place is private and a registered email address or phone \
                    \number is not whitelisted, a 403 error is returned."
        Doc.body (Doc.ref Doc.newUser) $
            Doc.description "JSON body"
        Doc.returns (Doc.ref Doc.user)
        Doc.response 201 "User created and pending activation." Doc.end
        Doc.errorResponse whitelistError
        Doc.errorResponse invalidInvitationCode
        Doc.errorResponse missingIdentity
        Doc.errorResponse userKeyExists
        Doc.errorResponse activationCodeNotFound
        Doc.errorResponse blacklistedEmail
        Doc.errorResponse blacklistedPhone

    ---

    get "/activate" (continue activate') $
        query "key"
        .&. query "code"

    document "GET" "activate" $ do
        Doc.summary "Activate (i.e. confirm) an email address or phone number."
        Doc.notes "See also 'POST /activate' which has a larger feature set."
        Doc.parameter Doc.Query "key" Doc.bytes' $
            Doc.description "Activation key"
        Doc.parameter Doc.Query "code" Doc.bytes' $
            Doc.description "Activation code"
        Doc.returns (Doc.ref Doc.activationResponse)
        Doc.response 200 "Activation successful." Doc.end
        Doc.response 204 "A recent activation was already successful." Doc.end
        Doc.errorResponse activationCodeNotFound

    ---

    post "/activate" (continue activateKey) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. request

    document "POST" "activate" $ do
        Doc.summary "Activate (i.e. confirm) an email address or phone number."
        Doc.notes "Activation only succeeds once and the number of \
                  \failed attempts for a valid key is limited."
        Doc.body (Doc.ref Doc.activate) $
            Doc.description "JSON body"
        Doc.returns (Doc.ref Doc.activationResponse)
        Doc.response 200 "Activation successful." Doc.end
        Doc.response 204 "A recent activation was already successful." Doc.end
        Doc.errorResponse activationCodeNotFound

    ---

    post "/activate/send" (continue sendActivationCode) $
        contentType "application" "json"
        .&. request

    document "POST" "sendActivationCode" $ do
        Doc.summary "Send (or resend) an email or phone activation code."
        Doc.body (Doc.ref Doc.sendActivationCode) $
            Doc.description "JSON body"
        Doc.response 200 "Activation code sent." Doc.end
        Doc.errorResponse invalidEmail
        Doc.errorResponse invalidPhone
        Doc.errorResponse userKeyExists
        Doc.errorResponse blacklistedEmail
        Doc.errorResponse blacklistedPhone

    ---

    post "/password-reset" (continue beginPasswordReset) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. request

    document "POST" "beginPasswordReset" $ do
        Doc.summary "Initiate a password reset."
        Doc.body (Doc.ref Doc.newPasswordReset) $
            Doc.description "JSON body"
        Doc.response 201 "Password reset code created and sent by email." Doc.end
        Doc.errorResponse invalidPwResetKey
        Doc.errorResponse duplicatePwResetCode

    ---

    post "/password-reset/complete" (continue completePasswordReset) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. request

    document "POST" "completePasswordReset" $ do
        Doc.summary "Complete a password reset."
        Doc.body (Doc.ref Doc.completePasswordReset) $
            Doc.description "JSON body"
        Doc.response 200 "Password reset successful." Doc.end
        Doc.errorResponse invalidPwResetCode

    ---

    post "/password-reset/:key" (continue deprecatedCompletePasswordReset) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. capture "key"
        .&. request

    document "POST" "deprecatedCompletePasswordReset" $ do
        Doc.deprecated
        Doc.summary "Complete a password reset."
        Doc.notes "DEPRECATED: Use 'POST /password-reset/complete'."

    ---

    post "/onboarding/v3" (continue onboarding) $
        accept "application" "json"
        .&. contentType "application" "json"
        .&. header "Z-User"
        .&. request

    document "POST" "onboardingV3" $ do
        Doc.summary "Upload contacts and invoke matching. Returns the list of Matches"
        Doc.body (Doc.ref Doc.addressBook) $ Doc.description "Address book"
        Doc.returns (Doc.ref Doc.onboardingMatches)
        Doc.response 200 "Matches" Doc.end

    -----

    Provider.routes
    Auth.routes
    Search.routes
    Team.routes
    TURN.routes

---------------------------------------------------------------------------
-- Handlers

setProperty :: UserId ::: ConnId ::: PropertyKey ::: Request ::: JSON -> Handler Response
setProperty (u ::: c ::: k ::: req ::: _) = do
    unless (Text.compareLength (Ascii.toText (propertyKeyName k)) maxKeyLen <= EQ) $
        throwStd propertyKeyTooLarge
    lbs <- Lazy.take (maxValueLen + 1) <$> liftIO (lazyRequestBody req)
    unless (Lazy.length lbs <= maxValueLen) $
        throwStd propertyValueTooLarge
    val <- hoistEither $ fmapL (StdError . badRequest . pack) (parsePropertyValue lbs)
    API.setProperty u c k val !>> propDataError
    return empty
  where
    maxKeyLen   = 256
    maxValueLen = 512

deleteProperty :: UserId ::: ConnId ::: PropertyKey -> Handler Response
deleteProperty (u ::: c ::: k) = lift (API.deleteProperty u c k) >> return empty

clearProperties :: UserId ::: ConnId -> Handler Response
clearProperties (u ::: c) = lift (API.clearProperties u c) >> return empty

getProperty :: UserId ::: PropertyKey ::: JSON -> Handler Response
getProperty (u ::: k ::: _) = do
    val <- lift $ API.lookupProperty u k
    return $ case val of
        Nothing -> setStatus status404 empty
        Just  v -> json v

listPropertyKeys :: UserId ::: JSON -> Handler Response
listPropertyKeys (u ::: _) = json <$> lift (API.lookupPropertyKeys u)

getPrekey :: UserId ::: ClientId ::: JSON -> Handler Response
getPrekey (u ::: c ::: _) = do
    prekey <- lift $ API.claimPrekey u c
    return $ case prekey of
        Just pk -> json pk
        Nothing -> setStatus status404 empty

getPrekeyBundle :: UserId ::: JSON -> Handler Response
getPrekeyBundle (u ::: _) = json <$> lift (API.claimPrekeyBundle u)

getMultiPrekeyBundles :: Request ::: JSON ::: JSON -> Handler Response
getMultiPrekeyBundles (req ::: _) = do
    body <- parseJsonBody req
    maxSize <- fromIntegral . setMaxConvAndTeamSize <$> view settings
    when (Map.size (userClients body) > maxSize) $
        throwStd tooManyClients
    json <$> lift (API.claimMultiPrekeyBundles body)

addClient :: Request ::: UserId ::: ConnId ::: Maybe IpAddr ::: JSON ::: JSON -> Handler Response
addClient (req ::: usr ::: con ::: ip ::: _) = do
    new <- parseJsonBody req
    clt <- API.addClient usr con (ipAddr <$> ip) new !>> clientError
    return . setStatus status201
           . addHeader "Location" (toByteString' $ clientId clt)
           $ json clt

rmClient :: Request ::: UserId ::: ConnId ::: ClientId ::: JSON ::: JSON -> Handler Response
rmClient (req ::: usr ::: con ::: clt ::: _) = do
    body <- parseJsonBody req
    API.rmClient usr con clt (rmPassword body) !>> clientError
    return empty

updateClient :: Request ::: UserId ::: ClientId ::: JSON ::: JSON -> Handler Response
updateClient (req ::: usr ::: clt ::: _) = do
    body <- parseJsonBody req
    API.updateClient usr clt body !>> clientError
    return empty

listClients :: UserId ::: JSON -> Handler Response
listClients (usr ::: _) = json <$> lift (API.lookupClients usr)

internalListClients :: JSON ::: JSON ::: Request -> Handler Response
internalListClients (_ ::: _ ::: req) = do
    UserSet usrs <- parseJsonBody req
    ucs <- Map.fromList <$> lift (API.lookupUsersClientIds $ Set.toList usrs)
    return $ json (UserClients ucs)

getClient :: UserId ::: ClientId ::: JSON -> Handler Response
getClient (usr ::: clt ::: _) = lift $ do
    client <- API.lookupClient usr clt
    return $ case client of
        Just c  -> json c
        Nothing -> setStatus status404 empty

getUserClients :: UserId ::: JSON -> Handler Response
getUserClients (user ::: _) =
    json <$> lift (fmap API.pubClient <$> API.lookupClients user)

getUserClient :: UserId ::: ClientId ::: JSON -> Handler Response
getUserClient (user ::: cid ::: _) = lift $ do
    client <- fmap API.pubClient <$> API.lookupClient user cid
    return $ case client of
        Just c  -> json c
        Nothing -> setStatus status404 empty

listPrekeyIds :: UserId ::: ClientId ::: JSON -> Handler Response
listPrekeyIds (usr ::: clt ::: _) = json <$> lift (API.lookupPrekeyIds usr clt)

autoConnect :: JSON ::: JSON ::: UserId ::: Maybe ConnId ::: Request -> Handler Response
autoConnect(_ ::: _ ::: uid ::: conn ::: req) = do
    UserSet to <- parseJsonBody req
    let num = Set.size to
    when (num < 1) $
        throwStd $ badRequest "No users given for auto-connect."
    when (num > 25) $
        throwStd $ badRequest "Too many users given for auto-connect (> 25)."
    conns <- API.autoConnect uid to conn !>> connError
    return $ json conns

createUser :: JSON ::: JSON ::: Request -> Handler Response
createUser (_ ::: _ ::: req) = do
    NewUserNoSSO new <- parseJsonBody req
    for_ (newUserEmail new) $ checkWhitelist . Left
    for_ (newUserPhone new) $ checkWhitelist . Right
    result <- API.createUser new !>> newUserError
    let acc   = createdAccount result
    let usr   = accountUser acc
    let eac   = createdEmailActivation result
    let pac   = createdPhoneActivation result
    let epair = (,) <$> (activationKey <$> eac) <*> (activationCode <$> eac)
    let ppair = (,) <$> (activationKey <$> pac) <*> (activationCode <$> pac)
    let lang  = userLocale usr
    lift $ do
        for_ (liftM2 (,) (userEmail usr) epair) $ \(e, p) ->
            sendActivationEmail e (userName usr) p (Just lang) (newUserTeam new)
        for_ (liftM2 (,) (userPhone usr) ppair) $ \(p, c) ->
            sendActivationSms p c (Just lang)
        for_ (liftM3 (,,) (userEmail usr) (createdUserTeam result) (newUserTeam new)) $ \(e, ct, ut) ->
            sendWelcomeEmail e ct ut (Just lang)
    cok <- case acc of
        UserAccount _ Ephemeral -> lift $ Auth.newCookie (userId usr) SessionCookie (newUserLabel new)
        UserAccount _ _         -> lift $ Auth.newCookie (userId usr) PersistentCookie (newUserLabel new)
    lift $ Auth.setResponseCookie cok
        $ setStatus status201
        . addHeader "Location" (toByteString' (userId usr))
        $ json (SelfProfile usr)
  where
    sendActivationEmail e u p l (Just (NewTeamCreator (BindingNewTeamUser (Team.BindingNewTeam t) _))) =
        sendTeamActivationMail e u p l (fromRange $ t^.Team.newTeamName)
    sendActivationEmail e u p l _ =
        sendActivationMail e u p l Nothing

    sendWelcomeEmail :: Email -> CreateUserTeam -> NewTeamUser -> Maybe Locale -> AppIO ()
    -- NOTE: Welcome e-mails for the team creator are not dealt by brig anymore
    sendWelcomeEmail _ (CreateUserTeam _ _) (NewTeamCreator _) _ = return ()
    sendWelcomeEmail e (CreateUserTeam t n) (NewTeamMember  _) l = Team.sendMemberWelcomeMail e t n l
    sendWelcomeEmail e (CreateUserTeam t n) (NewTeamMemberSSO _) l = Team.sendMemberWelcomeMail e t n l

createUserNoVerify :: JSON ::: JSON ::: Request -> Handler Response
createUserNoVerify (_ ::: _ ::: req) = do
    (uData :: NewUser)  <- parseJsonBody req
    result <- API.createUser uData !>> newUserError
    let acc = createdAccount result
    let usr = accountUser acc
    let uid = userId usr
    let eac = createdEmailActivation result
    let pac = createdPhoneActivation result
    for_ (catMaybes [eac, pac]) $ \adata ->
        let key  = ActivateKey $ activationKey adata
            code = activationCode adata
        in API.activate key code (Just uid) !>> actError
    return . setStatus status201
           . addHeader "Location" (toByteString' uid)
           $ json (SelfProfile usr)

deleteUserNoVerify :: UserId -> Handler Response
deleteUserNoVerify uid = do
    void $ lift (API.lookupAccount uid) >>= ifNothing userNotFound
    lift $ API.deleteUserNoVerify uid
    return $ setStatus status202 empty

changeSelfEmailNoSend :: JSON ::: UserId ::: Request -> Handler Response
changeSelfEmailNoSend (_ ::: u ::: req) = changeEmail u req False

checkUserExists :: UserId ::: UserId -> Handler Response
checkUserExists (self ::: uid) = do
    exists <- lift $ isJust <$> API.lookupProfile self uid
    if exists then return empty else throwStd userNotFound

getSelf :: JSON ::: UserId -> Handler Response
getSelf (_ ::: self) = do
    p <- (lift $ API.lookupSelfProfile self) >>= ifNothing userNotFound
    return $ json p

getUser :: JSON ::: UserId ::: UserId -> Handler Response
getUser (_ ::: self ::: uid) = do
    p <- (lift $ API.lookupProfile self uid) >>= ifNothing userNotFound
    return $ json p

getUserName :: JSON ::: UserId -> Handler Response
getUserName (_ ::: self) = do
    name <- lift $ API.lookupName self
    return $ case name of
        Just n  -> json $ object ["name" .= n]
        Nothing -> setStatus status404 empty

listUsers :: JSON ::: UserId ::: Either (List UserId) (List Handle) -> Handler Response
listUsers (_ ::: self ::: qry) = case qry of
    Left  us -> byIds (fromList us)
    Right hs -> do
        us <- mapM (lift . API.lookupHandle) (fromList hs)
        byIds (catMaybes us)
  where
    byIds uids = do
        profiles <- lift $ API.lookupProfiles self uids
        return $ case profiles of
            [] -> setStatus status404 empty
            ps -> json ps

listActivatedAccounts :: JSON ::: Either (List UserId) (List Handle) -> Handler Response
listActivatedAccounts (_ ::: qry) = case qry of
    Left  us -> byIds (fromList us)
    Right hs -> do
        us <- mapM (lift . API.lookupHandle) (fromList hs)
        byIds (catMaybes us)
  where
    byIds uids = json
               . filter (isJust . userIdentity . accountUser)
              <$> (lift $ API.lookupAccounts uids)

listAccountsByIdentity :: JSON ::: Either Email Phone -> Handler Response
listAccountsByIdentity (_ ::: emailOrPhone) = lift $ json
    <$> API.lookupAccountsByIdentity emailOrPhone

getActivationCode :: JSON ::: Either Email Phone -> Handler Response
getActivationCode (_ ::: emailOrPhone) = do
    apair <- lift $ API.lookupActivationCode emailOrPhone
    maybe (throwStd activationKeyNotFound) (return . found) apair
  where
    found (k, c) = json $ object [ "key" .= k, "code" .= c ]

getPasswordResetCode :: JSON ::: Either Email Phone -> Handler Response
getPasswordResetCode (_ ::: emailOrPhone) = do
    apair <- lift $ API.lookupPasswordResetCode emailOrPhone
    maybe (throwStd invalidPwResetKey) (return . found) apair
  where
    found (k, c) = json $ object [ "key" .= k, "code" .= c ]

updateUser :: JSON ::: UserId ::: ConnId ::: Request -> Handler Response
updateUser (_ ::: uid ::: conn ::: req) = do
    uu <- parseJsonBody req
    lift $ API.updateUser uid conn uu
    return empty

changeAccountStatus :: JSON ::: UserId ::: Request -> Handler Response
changeAccountStatus (_ ::: usr ::: req) = do
    status <- suStatus <$> parseJsonBody req
    API.changeAccountStatus (List1.singleton usr) status !>> accountStatusError
    return empty

getAccountStatus :: JSON ::: UserId -> Handler Response
getAccountStatus (_ ::: usr) = do
    status <- lift $ API.lookupStatus usr
    return $ case status of
        Just s  -> json $ object ["status" .= s]
        Nothing -> setStatus status404 empty

changePhone :: JSON ::: UserId ::: ConnId ::: Request -> Handler Response
changePhone (_ ::: u ::: _ ::: req) = do
    phone <- puPhone <$> parseJsonBody req
    (adata, pn) <- API.changePhone u phone !>> changePhoneError
    loc   <- lift $ API.lookupLocale u
    let apair = (activationKey adata, activationCode adata)
    lift $ sendActivationSms pn apair loc
    return $ setStatus status202 empty

removePhone :: UserId ::: ConnId -> Handler Response
removePhone (self ::: conn) = do
    API.removePhone self conn !>> idtError
    return empty

removeEmail :: UserId ::: ConnId -> Handler Response
removeEmail (self ::: conn) = do
    API.removeEmail self conn !>> idtError
    return empty

checkPasswordExists :: UserId -> Handler Response
checkPasswordExists self = do
    exists <- lift $ isJust <$> API.lookupPassword self
    return $ if exists then empty else setStatus status404 empty

changePassword :: JSON ::: UserId ::: Request -> Handler Response
changePassword (_ ::: u ::: req) = do
    cp <- parseJsonBody req
    API.changePassword u cp !>> changePwError
    return empty

changeLocale :: JSON ::: UserId ::: ConnId ::: Request -> Handler Response
changeLocale (_ ::: u ::: conn ::: req) = do
    l <- parseJsonBody req
    lift $ API.changeLocale u conn l
    return empty

checkHandle :: UserId ::: Text -> Handler Response
checkHandle (_ ::: h) = do
    handle <- validateHandle h
    owner <- lift $ API.lookupHandle handle
    if | isJust owner
        -- Handle is taken (=> getHandleInfo will return 200)
        -> return $ setStatus status200 empty
       | API.isBlacklistedHandle handle
        -- Handle is free but cannot be taken
        -> throwE (StdError invalidHandle)
       | otherwise
        -- Handle is free and can be taken
        -> return $ setStatus status404 empty

checkHandles :: JSON ::: JSON ::: UserId ::: Request -> Handler Response
checkHandles (_ ::: _ ::: _ ::: req) = do
    CheckHandles hs num <- parseJsonBody req
    let handles = mapMaybe parseHandle (fromRange hs)
    free <- lift $ API.checkHandles handles (fromRange num)
    return $ json free

getHandleInfo :: JSON ::: UserId ::: Handle -> Handler Response
getHandleInfo (_ ::: _ ::: h) = do
    owner <- lift $ API.lookupHandle h
    return $ case owner of
        Just  u -> json (UserHandleInfo u)
        Nothing -> setStatus status404 empty

changeHandle :: JSON ::: UserId ::: ConnId ::: Request -> Handler Response
changeHandle (_ ::: u ::: conn ::: req) = do
    HandleUpdate h <- parseJsonBody req
    handle <- validateHandle h
    API.changeHandle u conn handle !>> changeHandleError
    return empty

activateKey :: JSON ::: JSON ::: Request -> Handler Response
activateKey (_ ::: _ ::: req) = parseJsonBody req >>= activate

beginPasswordReset :: JSON ::: JSON ::: Request -> Handler Response
beginPasswordReset (_ ::: _ ::: req) = do
    NewPasswordReset target <- parseJsonBody req
    checkWhitelist target
    (u, pair) <- API.beginPasswordReset target !>> pwResetError
    loc       <- lift $ API.lookupLocale u
    lift $ case target of
        Left  email -> sendPasswordResetMail email pair loc
        Right phone -> sendPasswordResetSms  phone pair loc
    return $ setStatus status201 empty

completePasswordReset :: JSON ::: JSON ::: Request -> Handler Response
completePasswordReset (_ ::: _ ::: req) = do
    CompletePasswordReset{..} <- parseJsonBody req
    API.completePasswordReset cpwrIdent cpwrCode cpwrPassword !>> pwResetError
    return empty

sendActivationCode :: JSON ::: Request -> Handler Response
sendActivationCode (_ ::: req) = do
    SendActivationCode{..} <- parseJsonBody req
    checkWhitelist saUserKey
    API.sendActivationCode saUserKey saLocale saCall !>> sendActCodeError
    return empty

changeSelfEmail :: JSON ::: UserId ::: ConnId ::: Request -> Handler Response
changeSelfEmail (_ ::: u ::: _ ::: req) = changeEmail u req True

-- Deprecated and to be removed after new versions of brig and galley are
-- deployed. Reason for deprecation: it returns N^2 things (which is not
-- needed), it doesn't scale, and it accepts everything in URL parameters,
-- which doesn't work when the list of users is long.
deprecatedGetConnectionsStatus :: List UserId ::: Maybe Relation -> Handler Response
deprecatedGetConnectionsStatus (users ::: flt) = do
    r <- lift $ API.lookupConnectionStatus (fromList users) (fromList users)
    return . json $ maybe r (filterByRelation r) flt
  where
    filterByRelation l rel = filter ((==rel) . csStatus) l

getConnectionsStatus
    :: JSON ::: JSON ::: Request ::: Maybe Relation
    -> Handler Response
getConnectionsStatus (_ ::: _ ::: req ::: flt) = do
    ConnectionsStatusRequest{csrFrom, csrTo} <- parseJsonBody req
    r <- lift $ API.lookupConnectionStatus csrFrom csrTo
    return . json $ maybe r (filterByRelation r) flt
  where
    filterByRelation l rel = filter ((==rel) . csStatus) l

createConnection :: JSON ::: JSON ::: UserId ::: ConnId ::: Request -> Handler Response
createConnection (_ ::: _ ::: self ::: conn ::: req) = do
    cr <- parseJsonBody req
    rs <- API.createConnection self cr conn !>> connError
    return $ case rs of
        ConnectionCreated c -> setStatus status201 $ json c
        ConnectionExists  c -> json c

updateConnection :: JSON ::: JSON ::: UserId ::: ConnId ::: UserId ::: Request -> Handler Response
updateConnection (_ ::: _ ::: self ::: conn ::: other ::: req) = do
    newStatus <- cuStatus <$> parseJsonBody req
    mc <- API.updateConnection self other newStatus (Just conn) !>> connError
    return $ case mc of
        Just  c -> json c
        Nothing -> setStatus status204 empty

listConnections :: JSON ::: UserId ::: Maybe UserId ::: Range 1 500 Int32 -> Handler Response
listConnections (_ ::: uid ::: start ::: size) = json <$>
    lift (API.lookupConnections uid start size)

getConnection :: JSON ::: UserId ::: UserId -> Handler Response
getConnection (_ ::: uid ::: uid') = lift $ do
    conn <- API.lookupConnection uid uid'
    return $ case conn of
        Just c  -> json c
        Nothing -> setStatus status404 empty

revokeIdentity :: Either Email Phone -> Handler Response
revokeIdentity emailOrPhone = do
    lift $ API.revokeIdentity emailOrPhone
    return $ setStatus status200 empty

checkBlacklist :: Either Email Phone -> Handler Response
checkBlacklist emailOrPhone = do
    bl <- lift $ API.isBlacklisted emailOrPhone
    return $ setStatus (bool status404 status200 bl) empty

deleteFromBlacklist :: Either Email Phone -> Handler Response
deleteFromBlacklist emailOrPhone = do
    void . lift $ API.blacklistDelete emailOrPhone
    return empty

addBlacklist :: Either Email Phone -> Handler Response
addBlacklist emailOrPhone = do
    void . lift $ API.blacklistInsert emailOrPhone
    return empty

canBeDeleted :: UserId ::: TeamId -> Handler Response
canBeDeleted (uid ::: tid) = do
    onlyOwner <- lift (Team.teamOwnershipStatus uid tid)
    case onlyOwner of
       Team.IsOnlyTeamOwner       -> throwStd noOtherOwner
       Team.IsOneOfManyTeamOwners -> pure ()
       Team.IsNotTeamOwner        -> pure ()
       Team.NoTeamOwnersAreLeft   -> do  -- (keeping the user won't help in this case)
           Log.warn $ Log.field "user" (toByteString uid)
                    . Log.msg (Log.val "Team.NoTeamOwnersAreLeft")
    return empty

isTeamOwner :: UserId ::: TeamId -> Handler Response
isTeamOwner (uid ::: tid) = do
    onlyOwner <- lift (Team.teamOwnershipStatus uid tid)
    case onlyOwner of
       Team.IsOnlyTeamOwner       -> pure ()
       Team.IsOneOfManyTeamOwners -> pure ()
       Team.IsNotTeamOwner        -> throwStd insufficientTeamPermissions
       Team.NoTeamOwnersAreLeft   -> throwStd insufficientTeamPermissions
    return empty

deleteUser :: UserId ::: Request ::: JSON ::: JSON -> Handler Response
deleteUser (u ::: r ::: _ ::: _) = do
    body <- parseJsonBody r
    res <- API.deleteUser u (deleteUserPassword body) !>> deleteUserError
    return $ case res of
        Nothing  -> setStatus status200 empty
        Just ttl -> setStatus status202 (json (DeletionCodeTimeout ttl))

verifyDeleteUser :: Request ::: JSON ::: JSON -> Handler Response
verifyDeleteUser (r ::: _) = do
    body <- parseJsonBody r
    API.verifyDeleteUser body !>> deleteUserError
    return (setStatus status200 empty)

onboarding :: JSON ::: JSON ::: UserId ::: Request -> Handler Response
onboarding (_ ::: _ ::: uid ::: r) = do
    ab <- parseJsonBody r
    json <$> API.onboarding uid ab !>> connError

getContactList :: JSON ::: UserId -> Handler Response
getContactList (_ ::: uid) = do
    contacts <- lift $ API.lookupContactList uid
    return $ json $ (UserIds contacts)

-- Deprecated

deprecatedCompletePasswordReset :: JSON ::: JSON ::: PasswordResetKey ::: Request -> Handler Response
deprecatedCompletePasswordReset (_ ::: _ ::: k ::: req) = do
    pwr <- parseJsonBody req
    API.completePasswordReset (PasswordResetIdentityKey k) (pwrCode pwr) (pwrPassword pwr) !>> pwResetError
    return empty

-- Utilities

activate' :: ActivationKey ::: ActivationCode -> Handler Response
activate' (k ::: c) = activate $ Activate (ActivateKey k) c False

activate :: Activate -> Handler Response
activate (Activate tgt code dryrun)
    | dryrun = do
        API.preverify tgt code !>> actError
        return empty
    | otherwise = do
        result <- API.activate tgt code Nothing !>> actError
        return $ case result of
            ActivationSuccess ident first -> respond ident first
            ActivationPass                -> setStatus status204 empty
  where
    respond (Just ident) first = setStatus status200 $ json (ActivationResponse ident first)
    respond Nothing      _     = setStatus status200 empty

changeEmail :: UserId -> Request -> Bool -> Handler Response
changeEmail u req sendOutEmail = do
    email <- euEmail <$> parseJsonBody req
    API.changeEmail u email !>> changeEmailError >>= \case
        ChangeEmailIdempotent                       -> respond status204
        ChangeEmailNeedsActivation (usr, adata, en) -> handleActivation usr adata en
  where
    respond = return . flip setStatus empty
    handleActivation usr adata en = do
        when sendOutEmail $ do
            let apair = (activationKey adata, activationCode adata)
            let name  = userName usr
            let ident = userIdentity usr
            let lang  = userLocale usr
            lift $ sendActivationMail en name apair (Just lang) ident
        respond status202

validateHandle :: Text -> Handler Handle
validateHandle = maybe (throwE (StdError invalidHandle)) return . parseHandle

ifNothing :: Utilities.Error -> Maybe a -> Handler a
ifNothing e = maybe (throwStd e) return
