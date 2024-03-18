{-# LANGUAGE BangPatterns #-}
import Data.HashMap.Strict qualified as Map

import Data.ByteString.Char8 qualified as BS

import Data.ByteString.Lazy.Char8 qualified as LBS

import Control.Concurrent.Chan.Unagi
import Control.Concurrent (forkIO, getNumCapabilities)

import Control.Concurrent.Async ( replicateConcurrently )

import GHC.Clock                ( getMonotonicTime )
import Control.Monad (forM_)
import Debug.Trace (trace)

data Entry = Entry
  { _sum :: {-# UNPACK #-} !Int
  , _n   :: {-# UNPACK #-} !Int
  , _min :: {-# UNPACK #-} !Int
  , _max :: {-# UNPACK #-} !Int
  } deriving (Eq, Ord, Show)

instance Semigroup Entry where
  (Entry s1 n1 m1 mm1 ) <> (Entry s2 n2 m2 mm2)
    = Entry (s1 + s2) (n1+n2) (min m1 m2) (max mm1 mm2)

instance Monoid Entry where
  mempty = Entry 0 0 (maxBound) (minBound)

newEntry t = Entry t 1 t t


readThread :: InChan (Maybe LBS.ByteString) -> IO ()
readThread ic = do
  c <- LBS.readFile "measurements.txt"
  writeList2Chan ic . map (Just) $ LBS.lines c
  writeChan ic Nothing


parseEntry :: BS.ByteString -> (BS.ByteString, Entry)
parseEntry !bs = (station, newEntry $! intHigh * 10 + intLow)
  where
    (station, rest) = BS.span (/=';') bs
    (intHigh, rest2) = case BS.readInt (BS.drop 1 rest) of
      Nothing -> error $ "can't parse: " ++ BS.unpack bs ++ BS.unpack rest
      Just t -> t
    Just (intLow, _) = BS.readInt (BS.drop 1 rest2)

insertEntry :: Semigroup a => a
            -> Maybe a
            -> Maybe a
insertEntry !e Nothing = Just e
insertEntry !e1 (Just !e2) = Just $! e1 <> e2

calcThread :: InChan (Maybe a)
           -> OutChan (Maybe LBS.ByteString)
           -> Map.HashMap BS.ByteString Entry
           -> IO (Map.HashMap BS.ByteString Entry)
calcThread ic oc = go
  where go !m = do
          mbs <- readChan oc
          case mbs of
            Just bs -> do
                let (!s, !e) = parseEntry $ LBS.toStrict bs
                go $! Map.alter (insertEntry e) s m
            Nothing -> writeChan ic Nothing >> return m

  

main :: IO ()
main = do
  cpus <- getNumCapabilities
  (ic, oc) <- newChan

  forkIO $ readThread ic
  result <- replicateConcurrently cpus $ calcThread ic oc Map.empty

  mapM_ print $ Map.toList $ foldr (Map.unionWith (<>)) Map.empty result
