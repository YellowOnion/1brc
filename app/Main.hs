{-# LANGUAGE BangPatterns #-}

import Data.ByteString.Lazy.Char8 qualified as LBS
import Data.HashMap.Strict        qualified as Map
import Data.ByteString.Char8      qualified as BS
import Data.Foldable                              ( Foldable(foldl') )

import Control.Concurrent.Chan.Unagi.Bounded
    ( newChan, readChan, writeChan, InChan, OutChan )
import Control.Concurrent.Async ( forConcurrently )
import Control.Concurrent       ( forkIO, getNumCapabilities, setNumCapabilities )
import Control.Monad            ( replicateM, zipWithM_ )

import System.Environment (getArgs)
import System.IO ( hClose, openFile, IOMode(ReadMode), Handle )


qUEUEdEPTH :: Int
qUEUEdEPTH = 1024

data Entry = Entry
  { _sum :: {-# UNPACK #-} !Int
  , _n   :: {-# UNPACK #-} !Int
  , _min :: {-# UNPACK #-} !Int
  , _max :: {-# UNPACK #-} !Int
  } deriving (Eq, Ord)

instance Semigroup Entry where
  (Entry s1 n1 m1 mm1 ) <> (Entry s2 n2 m2 mm2)
    = Entry (s1 + s2) (n1+n2) (min m1 m2) (max mm1 mm2)

instance Monoid Entry where
  mempty = Entry 0 0 maxBound minBound

instance Show Entry where
  show Entry{..} = showFixed _min +/+ showFixed (_sum `div` _n) +/+ showFixed _max
    where
      a +/+ b = a ++ "/" ++ b
      showFixed i = let (hi, lo) = i `divMod` 10 in show hi ++ "." ++ show lo


newEntry :: Int -> Entry
newEntry t = Entry t 1 t t


openMeasurements :: IO Handle
openMeasurements = openFile "measurements.txt" ReadMode


readThread' :: [InChan (Maybe BS.ByteString)] -> IO ()
readThread' ics = do
  h <- openMeasurements
  c <- LBS.hGetContents h
  let ls = map (Just . LBS.toStrict) . LBS.lines $ c
  zipWithM_ writeChan (cycleWithDepth ics) ls
  mapM_ (`writeChan` Nothing) ics
  hClose h
  where
    cycleWithDepth :: [a] -> [a]
    cycleWithDepth = cycle
                    . foldl'
                      (\l i -> replicate (qUEUEdEPTH `div` 2) i ++ l)
                      []


-- {-# INLINE parseEntry #-}
parseEntry :: BS.ByteString -> (BS.ByteString, Entry)
parseEntry bs = (station, newEntry $! intHigh * 10 + intLow)
  where
  (station, rest) = BS.span (/=';') bs
  (intHigh, rest2) = case BS.readInt (BS.drop 1 rest) of
    Nothing -> error $ "can't parse: " ++ BS.unpack bs ++ BS.unpack rest
    Just t -> t
  (intLow, _) =  case BS.readInt (BS.drop 1 rest2) of
    Just t -> t
    Nothing -> error $ "didn't parse: " ++ BS.unpack bs ++ BS.unpack rest2


calcThread :: OutChan (Maybe BS.ByteString)
           -> Map.HashMap BS.ByteString Entry
           -> IO (Map.HashMap BS.ByteString Entry)
calcThread oc = go
  where go !m = do
          mbs <- readChan oc
          case mbs of
            Just bs -> do
                go $ Map.unionWith (<>) (uncurry Map.singleton $ parseEntry bs) m
            Nothing -> return m


-- | 28s  -N1
-- | 42s  -N24 ??
-- | use +RTS -qm for best performance
singleThreaded :: IO ()
singleThreaded = do
  setNumCapabilities 1
  c <- LBS.hGetContents =<< openMeasurements
  mapM_ print . Map.toList
    $ foldl' (flip $ Map.unionWith (<>) . (uncurry Map.singleton . parseEntry)) Map.empty
    $ map LBS.toStrict $ LBS.lines c


-- | 14s -N4
-- | 23s -N12 ???
multiThreaded :: IO ()
multiThreaded = do
  cpus <- getNumCapabilities
  -- | we need N + 1 system threads, it's a lot better to run the reader thread
  -- | on it's own system thread.
  setNumCapabilities $ cpus + 1
  chans <- replicateM cpus $ newChan qUEUEdEPTH
  let inchans = map fst chans
      outchans = map snd chans

  _ <- forkIO $ readThread' inchans
  result <- forConcurrently outchans (`calcThread` Map.empty)

  mapM_ print $ Map.toList $ foldr (Map.unionWith (<>)) Map.empty result


main :: IO ()
main = do
  version <- getArgs

  case version of
    ["m"] -> multiThreaded
    ["s"] -> singleThreaded
    _ -> return ()
