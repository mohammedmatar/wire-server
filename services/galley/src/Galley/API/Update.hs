{-# LANGUAGE ConstraintKinds   #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}

module Galley.API.Update
    ( -- * Managing Conversations
      updateConversation
    , acceptConv
    , blockConv
    , unblockConv
    , checkReusableCode
    , joinConversationById
    , joinConversationByReusableCode
    , addCode
    , rmCode
    , getCode
    , updateConversationAccess
    , updateConversationMessageTimer

      -- * Managing Members
    , Galley.API.Update.addMembers
    , updateMember
    , removeMember

      -- * Talking
    , postOtrMessage
    , postProtoOtrMessage
    , postOtrBroadcast
    , postProtoOtrBroadcast
    , isTyping

      -- * External Services
    , addService
    , rmService
    , Galley.API.Update.addBot
    , rmBot
    , postBotMessage
    ) where


import Control.Applicative hiding (empty)
import Control.Concurrent.Lifted (fork)
import Control.Lens
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Trans (lift)
import Control.Monad.State
import Data.Bool (bool)
import Data.Code
import Data.Foldable
import Data.Set (Set)
import Data.List ((\\))
import Data.Id
import Data.List1 (singleton)
import Data.Maybe (fromMaybe, catMaybes)
import Data.List1
import Data.Text (Text)
import Data.Time
import Galley.App
import Galley.API.Error
import Galley.API.Mapping
import Galley.API.Util
import Galley.Data.Services as Data
import Galley.Data.Types
import Galley.Intra.Push
import Galley.Intra.User
import Galley.Options
import Galley.Types
import Galley.Types.Bot
import Galley.Types.Clients (Clients)
import Galley.Types.Teams hiding (EventType (..), EventData (..), Event)
import Galley.Validation
import Network.HTTP.Types
import Network.Wai
import Network.Wai.Predicate hiding (_1, _2, setStatus, failure)
import Network.Wai.Utilities
import Prelude hiding (any, elem, head)

import qualified Brig.Types.User      as User
import qualified Data.Map.Strict      as Map
import qualified Data.Set             as Set
import qualified Galley.Data          as Data
import qualified Galley.External      as External
import qualified Galley.Intra.Client  as Intra
import qualified Galley.Types.Clients as Clients
import qualified Galley.Types.Proto   as Proto
import qualified Galley.API.Teams     as Teams

acceptConv :: UserId ::: Maybe ConnId ::: ConvId -> Galley Response
acceptConv (usr ::: conn ::: cnv) = do
    conv  <- Data.conversation cnv >>= ifNothing convNotFound
    conv' <- acceptOne2One usr conv conn
    setStatus status200 . json <$> conversationView usr conv'

blockConv :: UserId ::: ConvId -> Galley Response
blockConv (usr ::: cnv) = do
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    unless (Data.convType conv `elem` [ConnectConv, One2OneConv]) $
        throwM $ invalidOp "block: invalid conversation type"
    let mems  = Data.convMembers conv
    when (usr `isMember` mems) $ Data.removeMember usr cnv
    return empty

unblockConv :: UserId ::: Maybe ConnId ::: ConvId -> Galley Response
unblockConv (usr ::: conn ::: cnv) = do
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    unless (Data.convType conv `elem` [ConnectConv, One2OneConv]) $
        throwM $ invalidOp "unblock: invalid conversation type"
    conv' <- acceptOne2One usr conv conn
    setStatus status200 . json <$> conversationView usr conv'

updateConversationAccess :: UserId ::: ConnId ::: ConvId ::: Request ::: JSON -> Galley Response
updateConversationAccess (usr ::: zcon ::: cnv ::: req ::: _ ) = do
    body <- fromBody req invalidPayload :: Galley ConversationAccessUpdate
    let targetAccess = Set.fromList (toList (cupAccess body))
        targetRole = cupAccessRole body
    -- 'PrivateAccessRole' is for self-conversations, 1:1 conversations and
    -- so on; users are not supposed to be able to make other conversations
    -- have 'PrivateAccessRole'
    when (PrivateAccess `elem` targetAccess || PrivateAccessRole == targetRole) $
        throwM invalidTargetAccess
    -- The user who initiated access change has to be a conversation member
    (bots, users) <- botsAndUsers <$> Data.members cnv
    ensureConvMember users usr
    -- The conversation has to be a group conversation
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    ensureGroupConv conv
    -- Team conversations incur another round of checks
    case Data.convTeam conv of
        Just tid -> checkTeamConv tid
        Nothing  -> when (targetRole == TeamAccessRole) $ throwM invalidTargetAccess
    -- When there is no update to be done, we return 204; otherwise we go
    -- with 'uncheckedUpdateConversationAccess', which will potentially kick
    -- out some users and do DB updates.
    let currentAccess = Set.fromList (toList $ Data.convAccess conv)
        currentRole = Data.convAccessRole conv
    if currentAccess == targetAccess && currentRole == targetRole
        then return $ empty & setStatus status204
        else uncheckedUpdateConversationAccess body usr zcon conv
                 (currentAccess, targetAccess)
                 (currentRole,   targetRole)
                 users bots
  where
    checkTeamConv tid = do
        tMembers <- Data.teamMembers tid
        -- Only team members can change access mode
        unless (usr `elem` (view userId <$> tMembers)) $
            throwM accessDenied
        -- Access mode change for managed conversation is not allowed
        tcv <- Data.teamConversation tid cnv
        when (maybe False (view managedConversation) tcv) $
            throwM invalidManagedConvOp
        -- Access mode change might result in members being removed from the
        -- conversation, so the user must have the necessary permission flag
        void $ permissionCheck usr RemoveConversationMember tMembers

uncheckedUpdateConversationAccess
    :: ConversationAccessUpdate -> UserId -> ConnId -> Data.Conversation
    -> (Set Access, Set Access) -> (AccessRole, AccessRole)
    -> [Member] -> [BotMember]
    -> Galley Response
uncheckedUpdateConversationAccess body usr zcon conv (currentAccess, targetAccess) (currentRole, targetRole) users bots = do
    let cnv = convId conv
    -- Remove conversation codes if CodeAccess is revoked
    when (CodeAccess `elem` currentAccess && CodeAccess `notElem` targetAccess) $ do
        key <- mkKey cnv
        Data.deleteCode key ReusableCode

    -- Depending on a variety of things, some bots and users have to be
    -- removed from the conversation. We keep track of them using 'State'.
    (newUsers, newBots) <- flip execStateT (users, bots) $ do
        -- We might have to remove non-activated members
        when (currentRole > ActivatedAccessRole && targetRole <= ActivatedAccessRole) $ do
            mIds <- map memId <$> use usersL
            activated <- fmap User.userId <$> lift (lookupActivatedUsers mIds)
            usersL %= filter (\user -> memId user `elem` activated)
        -- In a team-only conversation we also want to remove bots and guests
        case (targetRole, Data.convTeam conv) of
            (TeamAccessRole, Just tid) -> do
                tMembers <- map (view userId) <$> lift (Data.teamMembers tid)
                usersL %= filter (\user -> memId user `elem` tMembers)
                botsL .= []
            _ -> return ()

    -- Update Cassandra & send an event
    now <- liftIO getCurrentTime
    let accessEvent = Event ConvAccessUpdate cnv usr now (Just $ EdConvAccessUpdate body)
    Data.updateConversationAccess cnv targetAccess targetRole
    pushEvent accessEvent users bots zcon

    -- Remove users and bots
    let removedUsers = map memId users \\ map memId newUsers
        removedBots  = map botMemId bots \\ map botMemId newBots
    mapM_ (deleteBot cnv) removedBots
    case removedUsers of
        []   -> return ()
        x:xs -> do
            e <- Data.removeMembers conv usr (list1 x xs)
            -- push event to all clients, including zconn
            -- since updateConversationAccess generates a second (member removal) event here
            for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> users)) $ \p -> push1 p
            void . fork $ void $ External.deliver (newBots `zip` repeat e)

    -- Return the event
    return $ json accessEvent & setStatus status200

  where
    usersL :: Lens' ([Member], [BotMember]) [Member]
    usersL = _1
    botsL :: Lens' ([Member], [BotMember]) [BotMember]
    botsL = _2

updateConversationMessageTimer :: UserId ::: ConnId ::: ConvId ::: Request ::: JSON -> Galley Response
updateConversationMessageTimer (usr ::: zcon ::: cnv ::: req ::: _ ) = do
    body <- fromBody req invalidPayload :: Galley ConversationMessageTimerUpdate
    let messageTimer = cupMessageTimer body
    -- checks and balances
    (bots, users) <- botsAndUsers <$> Data.members cnv
    ensureConvMember users usr
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    ensureGroupConv conv
    traverse_ ensureTeamMember $ Data.convTeam conv -- only team members can change the timer
    let currentTimer = Data.convMessageTimer conv
    if currentTimer == messageTimer then
        return $ empty & setStatus status204
    else do
        -- update cassandra & send event
        now <- liftIO getCurrentTime
        let e = Event ConvMessageTimerUpdate cnv usr now (Just $ EdConvMessageTimerUpdate body)
        Data.updateConversationMessageTimer cnv messageTimer
        pushEvent e users bots zcon
        return $ json e & setStatus status200
  where
    ensureTeamMember tid = do
        tMembers <- Data.teamMembers tid
        unless (usr `elem` (view userId <$> tMembers)) $
            throwM accessDenied

pushEvent :: Event -> [Member] -> [BotMember] -> ConnId -> Galley ()
pushEvent e users bots zcon = do
    for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> users)) $ \p ->
        push1 $ p & pushConn ?~ zcon
    void . fork $ void $ External.deliver (bots `zip` repeat e)

addCode :: UserId ::: ConnId ::: ConvId -> Galley Response
addCode (usr ::: zcon ::: cnv) = do
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    ensureConvMember (Data.convMembers conv) usr
    ensureAccess conv CodeAccess
    let (bots, users) = botsAndUsers $ Data.convMembers conv
    key <- mkKey cnv
    mCode <- Data.lookupCode key ReusableCode
    case mCode of
        Nothing -> do
            c <- generate cnv ReusableCode (Timeout 3600 * 24 * 365) -- one year TODO: configurable
            Data.insertCode c
            now <- liftIO getCurrentTime
            res <- createCode c
            let e = Event ConvCodeUpdate cnv usr now (Just $ EdConvCodeUpdate res)
            pushEvent e users bots zcon
            return $ json e & setStatus status201
        Just c -> do
            res <- createCode c
            return $ json res & setStatus status200
  where
    createCode :: Code -> Galley ConversationCode
    createCode c = do
        urlPrefix <- view $ options . optSettings . setConversationCodeURI
        return $ mkConversationCode (codeKey c) (codeValue c) urlPrefix

rmCode :: UserId ::: ConnId ::: ConvId -> Galley Response
rmCode (usr ::: zcon ::: cnv) = do
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    ensureConvMember (Data.convMembers conv) usr
    ensureAccess conv CodeAccess
    let (bots, users) = botsAndUsers $ Data.convMembers conv
    key <- mkKey cnv
    Data.deleteCode key ReusableCode
    now <- liftIO getCurrentTime
    let e = Event ConvCodeDelete cnv usr now Nothing
    pushEvent e users bots zcon
    return $ json e & setStatus status200

getCode :: UserId ::: ConvId -> Galley Response
getCode (usr ::: cnv) = do
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    ensureAccess conv CodeAccess
    ensureConvMember (Data.convMembers conv) usr
    key <- mkKey cnv
    c <- Data.lookupCode key ReusableCode >>= ifNothing codeNotFound
    returnCode c

returnCode :: Code -> Galley Response
returnCode c = do
    urlPrefix <- view $ options . optSettings . setConversationCodeURI
    let res = mkConversationCode (codeKey c) (codeValue c) urlPrefix
    return $ setStatus status200 . json $ res

checkReusableCode :: Request ::: JSON -> Galley Response
checkReusableCode (req ::: _) = do
    convCode <- fromBody req invalidPayload
    void $ verifyReusableCode convCode
    return empty

joinConversationByReusableCode :: UserId ::: ConnId ::: Request ::: JSON -> Galley Response
joinConversationByReusableCode (zusr ::: zcon ::: req ::: _) = do
    convCode <- fromBody req invalidPayload
    c <- verifyReusableCode convCode
    joinConversation zusr zcon (codeConversation c) CodeAccess

verifyReusableCode :: ConversationCode -> Galley Code
verifyReusableCode convCode = do
    c <- Data.lookupCode (conversationKey convCode) ReusableCode >>= ifNothing codeNotFound
    unless (codeValue c == conversationCode convCode) $
        throwM codeNotFound
    return c

joinConversationById :: UserId ::: ConnId ::: ConvId ::: JSON -> Galley Response
joinConversationById (zusr ::: zcon ::: cnv ::: _) = joinConversation zusr zcon cnv LinkAccess

joinConversation :: UserId -> ConnId -> ConvId -> Access -> Galley Response
joinConversation zusr zcon cnv access = do
    conv <- Data.conversation cnv >>= ifNothing convNotFound
    ensureAccess conv access
    mbTms <- traverse Data.teamMembers $ Data.convTeam conv
    ensureAccessRole (Data.convAccessRole conv) [zusr] mbTms
    let newUsers = filter (notIsMember conv) [zusr]
    ensureMemberLimit (toList $ Data.convMembers conv) newUsers
    addToConversation (botsAndUsers (Data.convMembers conv)) zusr zcon newUsers conv

addMembers :: UserId ::: ConnId ::: ConvId ::: Request ::: JSON -> Galley Response
addMembers (zusr ::: zcon ::: cid ::: req ::: _) = do
    body <- fromBody req invalidPayload
    conv <- Data.conversation cid >>= ifNothing convNotFound
    let mems = botsAndUsers (Data.convMembers conv)
    toAdd <- fromMemberSize <$> checkedMemberAddSize (toList $ invUsers body)
    let newUsers = filter (notIsMember conv) (toList toAdd)
    ensureMemberLimit (toList $ Data.convMembers conv) newUsers
    ensureAccess conv InviteAccess
    case Data.convTeam conv of
        Nothing -> do
            ensureConvMember (snd mems) zusr
            ensureAccessRole (Data.convAccessRole conv) newUsers Nothing
            ensureConnected zusr newUsers
        Just ti -> teamConvChecks ti newUsers conv
    addToConversation mems zusr zcon newUsers conv
  where
    teamConvChecks tid newUsers conv = do
        tms <- Data.teamMembers tid
        ensureAccessRole (Data.convAccessRole conv) newUsers (Just tms)
        void $ permissionCheck zusr AddConversationMember tms
        tcv <- Data.teamConversation tid cid
        when (maybe True (view managedConversation) tcv) $
            throwM noAddToManaged
        let guests = notTeamMember newUsers tms
        ensureConnected zusr guests

updateMember :: UserId ::: ConnId ::: ConvId ::: Request ::: JSON -> Galley Response
updateMember (zusr ::: zcon ::: cid ::: req ::: _) = do
    alive <- Data.isConvAlive cid
    unless alive $ do
        Data.deleteConversation cid
        throwM convNotFound
    body <- fromBody req invalidPayload
    m    <- Data.member cid zusr >>= ifNothing convNotFound
    up   <- Data.updateMember cid zusr body
    now  <- liftIO getCurrentTime
    let e = Event MemberStateUpdate cid zusr now (Just $ EdMemberUpdate up)
    let ms = applyChanges m up
    for_ (newPush (evtFrom e) (ConvEvent e) [recipient ms]) $ \p ->
        push1 $ p
              & pushConn  ?~ zcon
              & pushRoute .~ RouteDirect
    return empty
  where
    applyChanges :: Member -> MemberUpdateData -> Member
    applyChanges m u = m
        { memOtrMuted       = fromMaybe (memOtrMuted m) (misOtrMuted u)
        , memOtrMutedRef    = misOtrMutedRef u <|> memOtrMutedRef m
        , memOtrArchived    = fromMaybe (memOtrArchived m) (misOtrArchived u)
        , memOtrArchivedRef = misOtrArchivedRef u <|> memOtrArchivedRef m
        , memHidden         = fromMaybe (memHidden m) (misHidden u)
        , memHiddenRef      = misHiddenRef u <|> memHiddenRef m
        }

removeMember :: UserId ::: ConnId ::: ConvId ::: UserId -> Galley Response
removeMember (zusr ::: zcon ::: cid ::: victim) = do
    conv <- Data.conversation cid >>= ifNothing convNotFound
    let (bots, users) = botsAndUsers (Data.convMembers conv)
    case Data.convTeam conv of
        Nothing -> regularConvChecks users
        Just ti -> teamConvChecks ti
    ensureGroupConv conv
    if victim `isMember` users then do
        e <- Data.removeMembers conv zusr (singleton victim)
        for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> users)) $ \p ->
            push1 $ p & pushConn ?~ zcon
        void . fork $ void $ External.deliver (bots `zip` repeat e)
        return $ json e & setStatus status200
    else
        return $ empty & setStatus status204
  where
    regularConvChecks users =
        unless (zusr `isMember` users) $ throwM convNotFound

    teamConvChecks tid = do
        unless (zusr == victim) $
            void $ permissionCheck zusr RemoveConversationMember =<< Data.teamMembers tid
        tcv <- Data.teamConversation tid cid
        when (maybe False (view managedConversation) tcv) $
            throwM (invalidOp "Users can not be removed from managed conversations.")

postBotMessage :: BotId ::: ConvId ::: OtrFilterMissing ::: Request ::: JSON ::: JSON -> Galley Response
postBotMessage (zbot ::: zcnv ::: val ::: req ::: _) = do
    msg <- fromBody req invalidPayload
    postNewOtrMessage (botUserId zbot) Nothing zcnv val msg

postProtoOtrMessage :: UserId ::: ConnId ::: ConvId ::: OtrFilterMissing ::: Request ::: Media "application" "x-protobuf" -> Galley Response
postProtoOtrMessage (zusr ::: zcon ::: cnv ::: val ::: req ::: _) =
    Proto.toNewOtrMessage <$> fromProtoBody req invalidPayload >>=
    postNewOtrMessage zusr (Just zcon) cnv val

postOtrMessage :: UserId ::: ConnId ::: ConvId ::: OtrFilterMissing ::: Request ::: JSON -> Galley Response
postOtrMessage (zusr ::: zcon ::: cnv ::: val ::: req ::: _) =
    postNewOtrMessage zusr (Just zcon) cnv val =<< fromBody req invalidPayload

postOtrBroadcast :: UserId ::: ConnId ::: OtrFilterMissing ::: Request ::: JSON -> Galley Response
postOtrBroadcast (zusr ::: zcon ::: val ::: req ::: _) =
    postNewOtrBroadcast zusr (Just zcon) val =<< fromBody req invalidPayload

postProtoOtrBroadcast :: UserId ::: ConnId ::: OtrFilterMissing ::: Request ::: JSON -> Galley Response
postProtoOtrBroadcast (zusr ::: zcon ::: val ::: req ::: _) =
    Proto.toNewOtrMessage <$> fromProtoBody req invalidPayload >>=
    postNewOtrBroadcast zusr (Just zcon) val

postNewOtrBroadcast :: UserId -> Maybe ConnId -> OtrFilterMissing -> NewOtrMessage -> Galley Response
postNewOtrBroadcast usr con val msg = do
    let sender = newOtrSender msg
    let recvrs = newOtrRecipients msg
    now <- liftIO getCurrentTime
    withValidOtrBroadcastRecipients usr sender recvrs val now $ \rs -> do
        let (_, toUsers) = foldr (newMessage usr con Nothing msg now) ([],[]) rs
        pushSome (catMaybes toUsers)
        -- bots are not supported on broadcast

postNewOtrMessage :: UserId -> Maybe ConnId -> ConvId -> OtrFilterMissing -> NewOtrMessage -> Galley Response
postNewOtrMessage usr con cnv val msg = do
    let sender = newOtrSender msg
    let recvrs = newOtrRecipients msg
    now <- liftIO getCurrentTime
    withValidOtrRecipients usr sender cnv recvrs val now $ \rs -> do
        let (toBots, toUsers) = foldr (newMessage usr con (Just cnv) msg now) ([],[]) rs
        pushSome (catMaybes toUsers)
        void . fork $ do
            gone <- External.deliver toBots
            mapM_ (deleteBot cnv . botMemId) gone

newMessage
    :: UserId
    -> Maybe ConnId
    -> Maybe ConvId  -- ^ Conversation Id (if Nothing, recipient's self conversation is used)
    -> NewOtrMessage
    -> UTCTime
    -> (Member, ClientId, Text)
    -> ([(BotMember, Event)], [Maybe Push])
    -> ([(BotMember, Event)], [Maybe Push])
newMessage usr con cnv msg now (m, c, t) ~(toBots, toUsers) =
    let o = OtrMessage
          { otrSender     = newOtrSender msg
          , otrRecipient  = c
          , otrCiphertext = t
          , otrData       = newOtrData msg
          }
        conv = fromMaybe (selfConv $ memId m) cnv -- use recipient's client's self conversation on broadcast
        e = Event OtrMessageAdd conv usr now (Just $ EdOtrMessage o)
        r = recipient m & recipientClients .~ [c]
    in case newBotMember m of
        Just  b -> ((b,e):toBots, toUsers)
        Nothing ->
            let p = newPush (evtFrom e) (ConvEvent e) [r] <&>
                    set pushConn con
                  . set pushNativePriority (newOtrNativePriority msg)
                  . set pushRoute          (bool RouteDirect RouteAny (newOtrNativePush msg))
                  . set pushTransient      (newOtrTransient msg)
            in (toBots, p:toUsers)

updateConversation :: UserId ::: ConnId ::: ConvId ::: Request ::: JSON -> Galley Response
updateConversation (zusr ::: zcon ::: cnv ::: req ::: _) = do
    body <- fromBody req invalidPayload
    alive <- Data.isConvAlive cnv
    unless alive $ do
        Data.deleteConversation cnv
        throwM convNotFound
    (bots, users) <- botsAndUsers <$> Data.members cnv
    unless (zusr `isMember` users) $
        throwM convNotFound
    now <- liftIO getCurrentTime
    cn  <- rangeChecked (cupName body)
    Data.updateConversation cnv cn
    let e = Event ConvRename cnv zusr now (Just $ EdConvRename body)
    for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> users)) $ \p ->
        push1 $ p & pushConn ?~ zcon
    void . fork $ void $ External.deliver (bots `zip` repeat e)
    return $ json e & setStatus status200

isTyping :: UserId ::: ConnId ::: ConvId ::: Request ::: JSON -> Galley Response
isTyping (zusr ::: zcon ::: cnv ::: req ::: _) = do
    body <- fromBody req invalidPayload
    mm   <- Data.members cnv
    unless (zusr `isMember` mm) $
        throwM convNotFound
    now <- liftIO getCurrentTime
    let e = Event Typing cnv zusr now (Just $ EdTyping body)
    for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> mm)) $ \p ->
        push1 $ p
              & pushConn      ?~ zcon
              & pushRoute     .~ RouteDirect
              & pushTransient .~ True
    return empty

addService :: Request ::: JSON -> Galley Response
addService (req ::: _) = do
    Data.insertService =<< fromBody req invalidPayload
    return empty

rmService :: Request ::: JSON -> Galley Response
rmService (req ::: _) = do
    Data.deleteService =<< fromBody req invalidPayload
    return empty

addBot :: UserId ::: ConnId ::: Request ::: JSON -> Galley Response
addBot (zusr ::: zcon ::: req ::: _) = do
    b <- fromBody req invalidPayload
    c <- Data.conversation (b^.addBotConv) >>= ifNothing convNotFound
    -- Check some preconditions on adding bots to a conversation
    for_ (Data.convTeam c) $ teamConvChecks (b^.addBotConv)
    (bots, users) <- regularConvChecks b c

    t <- liftIO getCurrentTime
    Data.updateClient True (botUserId (b^.addBotId)) (b^.addBotClient)
    (e, bm) <- Data.addBotMember zusr (b^.addBotService) (b^.addBotId) (b^.addBotConv) t
    for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> users)) $ \p ->
        push1 $ p & pushConn ?~ zcon
    void . fork $ void $ External.deliver ((bm:bots) `zip` repeat e)
    return (json e)
  where
    regularConvChecks b c = do
        let (bots, users) = botsAndUsers (Data.convMembers c)
        unless (zusr `isMember` users) $
            throwM convNotFound
        ensureGroupConv c
        unless (any ((== b^.addBotId) . botMemId) bots) $
            ensureMemberLimit (toList $ Data.convMembers c) [botUserId (b^.addBotId)]
        return (bots, users)

    teamConvChecks cid tid = do
        tms <- Data.teamMembers tid
        void $ permissionCheck zusr AddConversationMember tms
        tcv <- Data.teamConversation tid cid
        when (maybe True (view managedConversation) tcv) $
            throwM noAddToManaged

rmBot :: UserId ::: Maybe ConnId ::: Request ::: JSON -> Galley Response
rmBot (zusr ::: zcon ::: req ::: _) = do
    b <- fromBody req invalidPayload
    c <- Data.conversation (b^.rmBotConv) >>= ifNothing convNotFound
    unless (zusr `isMember` Data.convMembers c) $
        throwM convNotFound
    let (bots, users) = botsAndUsers (Data.convMembers c)
    if not (any ((== b^.rmBotId) . botMemId) bots)
        then return $ setStatus status204 empty
        else do
            t <- liftIO getCurrentTime
            let evd = Just (EdMembers (Members [botUserId (b^.rmBotId)]))
            let e = Event MemberLeave (Data.convId c) zusr t evd
            for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> users)) $ \p ->
                push1 $ p & pushConn .~ zcon
            Data.removeMember (botUserId (b^.rmBotId)) (Data.convId c)
            Data.eraseClients (botUserId (b^.rmBotId))
            void . fork $ void $ External.deliver (bots `zip` repeat e)
            return (json e)

-------------------------------------------------------------------------------
-- Helpers

addToConversation :: ([BotMember], [Member]) -> UserId -> ConnId -> [UserId] -> Data.Conversation -> Galley Response
addToConversation _              _   _    [] _ = return $ empty & setStatus status204
addToConversation (bots, others) usr conn xs c = do
    ensureGroupConv c
    mems    <- checkedMemberAddSize xs
    now     <- liftIO getCurrentTime
    (e, mm) <- Data.addMembers now (Data.convId c) usr mems
    for_ (newPush (evtFrom e) (ConvEvent e) (recipient <$> allMembers (toList mm))) $ \p ->
        push1 $ p & pushConn ?~ conn
    void . fork $ void $ External.deliver (bots `zip` repeat e)
    return $ json e & setStatus status200
  where
    allMembers new = foldl' fn new others
      where
        fn acc m
            | any ((== memId m) . memId) acc = acc
            | otherwise                      = m : acc

ensureGroupConv :: MonadThrow m => Data.Conversation -> m ()
ensureGroupConv c = case Data.convType c of
    SelfConv    -> throwM invalidSelfOp
    One2OneConv -> throwM invalidOne2OneOp
    ConnectConv -> throwM invalidConnectOp
    _           -> return ()

ensureMemberLimit :: [Member] -> [UserId] -> Galley ()
ensureMemberLimit old new = do
    o <- view options
    let maxSize = fromIntegral (o^.optSettings.setMaxConvSize)
    when (length old + length new > maxSize) $
        throwM tooManyMembers

notIsMember :: Data.Conversation -> UserId -> Bool
notIsMember cc u = not $ isMember u (Data.convMembers cc)

ensureConvMember :: [Member] -> UserId -> Galley ()
ensureConvMember users usr =
    unless (usr `isMember` users) $
        throwM convNotFound

ensureAccess :: Data.Conversation -> Access -> Galley ()
ensureAccess conv access =
    unless (access `elem` Data.convAccess conv) $
        throwM accessDenied

-------------------------------------------------------------------------------
-- OtrRecipients Validation

data CheckedOtrRecipients
    = ValidOtrRecipients !ClientMismatch [(Member, ClientId, Text)]
        -- ^ Valid sender (user and client) and no missing recipients,
        -- or missing recipients have been willfully ignored.
    | MissingOtrRecipients !ClientMismatch
        -- ^ Missing recipients.
    | InvalidOtrSenderUser
        -- ^ Invalid sender (user).
    | InvalidOtrSenderClient
        -- ^ Invalid sender (client).

withValidOtrBroadcastRecipients
    :: UserId
    -> ClientId
    -> OtrRecipients
    -> OtrFilterMissing
    -> UTCTime
    -> ([(Member, ClientId, Text)] -> Galley ())
    -> Galley Response
withValidOtrBroadcastRecipients usr clt rcps val now go = Teams.withBindingTeam usr $ \tid -> do
    tMembers <- fmap (view userId) <$> Data.teamMembers tid
    contacts <- getContactList usr
    let users = Set.toList $ Set.union (Set.fromList tMembers) (Set.fromList contacts)
    isInternal <- view $ options . optSettings . setIntraListing
    clts <- if isInternal then
                Clients.fromUserClients <$> Intra.lookupClients users
            else
                Data.lookupClients users
    let membs = Data.newMember <$> users
    handleOtrResponse usr clt rcps membs clts val now go

withValidOtrRecipients
    :: UserId
    -> ClientId
    -> ConvId
    -> OtrRecipients
    -> OtrFilterMissing
    -> UTCTime
    -> ([(Member, ClientId, Text)] -> Galley ())
    -> Galley Response
withValidOtrRecipients usr clt cnv rcps val now go = do
    alive <- Data.isConvAlive cnv
    unless alive $ do
        Data.deleteConversation cnv
        throwM convNotFound
    membs <- Data.members cnv
    let memIds = (memId <$> membs)
    isInternal <- view $ options . optSettings . setIntraListing
    clts <- if isInternal then
                Clients.fromUserClients <$> Intra.lookupClients memIds
            else
                Data.lookupClients memIds
    handleOtrResponse usr clt rcps membs clts val now go

handleOtrResponse
    :: UserId           -- ^ Proposed sender (user)
    -> ClientId         -- ^ Proposed sender (client)
    -> OtrRecipients    -- ^ Proposed recipients (users & clients).
    -> [Member]         -- ^ Members to consider as valid recipients.
    -> Clients          -- ^ Clients to consider as valid recipients.
    -> OtrFilterMissing -- ^ How to filter missing clients.
    -> UTCTime          -- ^ The current timestamp.
    -> ([(Member, ClientId, Text)] -> Galley ()) -- ^ Callback if OtrRecipients are valid
    -> Galley Response
handleOtrResponse usr clt rcps membs clts val now go = case checkOtrRecipients usr clt rcps membs clts val now of
        ValidOtrRecipients   m r -> go r >> return (json m & setStatus status201)
        MissingOtrRecipients m   -> return (json m & setStatus status412)
        InvalidOtrSenderUser     -> throwM convNotFound
        InvalidOtrSenderClient   -> throwM unknownClient

-- | Check OTR sender and recipients for validity and completeness
-- against a given list of valid members and clients, optionally
-- ignoring missing clients. Returns 'ValidOtrRecipients' on success
-- for further processing.
checkOtrRecipients
    :: UserId           -- ^ Proposed sender (user)
    -> ClientId         -- ^ Proposed sender (client)
    -> OtrRecipients    -- ^ Proposed recipients (users & clients).
    -> [Member]         -- ^ Members to consider as valid recipients.
    -> Clients          -- ^ Clients to consider as valid recipients.
    -> OtrFilterMissing -- ^ How to filter missing clients.
    -> UTCTime          -- ^ The current timestamp.
    -> CheckedOtrRecipients
checkOtrRecipients usr sid prs vms vcs val now
    | not (Map.member usr vmembers)      = InvalidOtrSenderUser
    | not (Clients.contains usr sid vcs) = InvalidOtrSenderClient
    | not (Clients.null missing)         = MissingOtrRecipients mismatch
    | otherwise                          = ValidOtrRecipients mismatch yield
  where
    yield = foldrOtrRecipients next [] prs

    next u c t rs
        | Just m <- member u c = (m, c, t) : rs
        | otherwise            = rs

    member u c
        | Just m <- Map.lookup u vmembers
        , Clients.contains u c vclients = Just m
        | otherwise                     = Nothing

    -- Valid recipient members & clients
    vmembers   = Map.fromList $ map (\m -> (memId m, m)) vms
    vclients   = Clients.rmClient usr sid vcs

    -- Proposed (given) recipients
    recipients = userClientMap (otrRecipientsMap prs)
    given      = Clients.fromMap (Map.map Map.keysSet recipients)

    -- Differences between valid and proposed recipients
    missing   = filterMissing (Clients.diff vclients given)
    unknown   = Clients.diff given vcs
    deleted   = Clients.filter (`Map.member` vmembers) unknown
    redundant = Clients.diff unknown deleted &
                    if Clients.contains usr sid given
                        then Clients.insert usr sid
                        else id

    mismatch = ClientMismatch
             { cmismatchTime    = now
             , missingClients   = UserClients (Clients.toMap missing)
             , redundantClients = UserClients (Clients.toMap redundant)
             , deletedClients   = UserClients (Clients.toMap deleted)
             }

    filterMissing miss = case val of
        OtrReportAllMissing -> miss
        OtrIgnoreAllMissing -> Clients.nil
        OtrReportMissing us -> Clients.filter (`Set.member` us) miss
        OtrIgnoreMissing us -> Clients.filter (`Set.notMember` us) miss
