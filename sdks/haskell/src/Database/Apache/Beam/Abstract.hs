{-# LANGUAGE PolyKinds, TypeInType, MultiParamTypeClasses, AllowAmbiguousTypes,
FlexibleInstances, FunctionalDependencies, ScopedTypeVariables, GADTs, UndecidableInstances #-}
module Database.Apache.Beam.Abstract where

import Data.Monoid
import Control.Monad.Except

-- Pipeline --------------------------------------------------------------------

data Pipeline e m (local :: Bool) (doesIO :: Bool) n = ProxyPipeline
instance Pipelinable e m l d n => Show (Pipeline e m l d n) where
  show _ = "[Pipe=" ++ pipelineName (ProxyPipeline :: Pipeline e m l d n) ++ "]"

class Monad m => Pipelinable e m local doesIO n where
  newPipeline :: m (Pipeline e m local doesIO n)
  newPipeline = return ProxyPipeline
  pipelineName :: Pipeline e m local doesIO n -> String
  execute :: (p ~ Pipeline e m local doesIO n) => PCollection b p a -> ExceptT e m ()
  executeWithResult :: (p ~ Pipeline e m local doesIO n) => PCollection b p a -> ExceptT e m [a]

data Source (b :: Bool) p s a where
  Empty :: Source 'True p s a
  CreateBounded :: Int -> [a] -> Source 'True p s a
  CreateUnbounded :: (p ~ Pipeline e m 'True d n) => [a] -> Source 'False p s a
  SourceFile :: (p ~ Pipeline e m b 'True n) => FilePath -> (s -> ExceptT e m a) -> Source 'True p s a
instance Show (Source b p s a) where
  show _ = "[Source]"

data Sink p where
  SinkFile :: FilePath -> Sink p
  deriving (Show)

-- PCollection of objects should probably remain abstract..
-- TODO: put that somewhere else
type Coll a = [a]

-- The Apache Beam Java API makes heavy use of KV<K, V>
-- In Haskell a tuple can be used
type KV k v = (,) k v

data WindowingMode b where
  GlobalWindowing :: WindowingMode 'True
  NonGlobalWindowing :: WindowingMode 'False
  AggregationTrigger :: WindowingMode 'False

data PTransform p b a b' a' where
  -- Utility functions that do not have any effect on execution
  -- Label a PTransformation, useful for debugging
  (:%) :: String -> PTransform p b a b' a' -> PTransform p b a b' a'
  -- PTransformations
  ParDo :: (a -> a') -> PTransform p b a b a'
  GroupByKey :: WindowingMode b -> PTransform p b (k, v) b (k, Coll v)
  CoGroupByKey :: (a ~ (k, v)) => WindowingMode b
                               -> PCollection b p (k, v')
                               -> PTransform p b (k, v) b (k, Coll (v, v')) -- TODO: we need some kind of object
  Combine :: Monoid a => WindowingMode b
                      -> PTransform p b a b a
  CombineWith :: WindowingMode b
              -> (a -> a -> a)
              -> PTransform p b a b a
  CombineFn :: Monoid acc => WindowingMode b
                          -> (a -> acc -> acc)    -- fold
                          -> (acc -> a')          -- extract result
                          -> PTransform p b a b a'
  CombinePerKey :: (a ~ (k, v), Eq k, Ord k)
                => (k -> Coll v -> v')
                -> PTransform p b a b (k, v')
  FlatMap :: (a -> [a']) -> PTransform p b a b a'
  Map :: (a -> a') -> PTransform p b a b a'
instance Show (PTransform p b a b' a') where
  show _ = "<PTransform>"

data PCollection b p a where
  (:>) :: PCollection b p a -> PTransform p b a b' a' -> PCollection b' p a'
  Flatten :: PCollections b p a -> PCollection b p a
  (:+) :: (p ~ Pipeline e m l d n) => p -> Source b p s a -> PCollection b p a
  (:-) :: PCollection b p a -> Sink p -> PCollection b p a
instance Functor (PCollection b p) where
  fmap f c = c :> Map f
instance (Show p) => Show (PCollection b p a) where
  show (c :> t) = show c ++ " :> " ++ show t
  show (Flatten c) = "Flatten " ++ show c
  show (p :+ s) = show p ++ " :+ " ++ show s
  show (c :- s) = show c ++ " :- " ++ show s

data PCollections b p a where
  Singleton :: PCollection b p a -> PCollections b p a
  List :: [PCollection b p a] -> PCollections b p a
  Partition :: PCollection b p a -> Int -> (a -> Int) -> PCollections b p a
instance Show (PCollection b p a) => Show (PCollections b p a) where
  show (Singleton c) = "Singletion " ++ show c
  show (List cs) = show cs
  show (Partition c i f) = "<Partition>"
