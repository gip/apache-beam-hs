{-# LANGUAGE PolyKinds, TypeInType, MultiParamTypeClasses, AllowAmbiguousTypes,
FlexibleInstances, FunctionalDependencies, ScopedTypeVariables, GADTs, UndecidableInstances #-}
module Database.Apache.Beam.Executor.LocalIO where

import Data.List
import Control.Monad.Identity
import Control.Monad.Except
import Database.Apache.Beam
import Database.Apache.Beam.Executor.Pure

data LocalIOPipeline = LocalIOPipeline

data LocalIOError s =
    SourceFileNotFound s
  | SourceFileParsingException s
  | SinkFileExists s
  | SinkFileException s
  deriving (Show, Eq)

instance Pipelinable (LocalIOError String) IO True True LocalIOPipeline where
  pipelineName _ = "LocalIO"
  execute c = executeLocal c >> return ()
  executeWithResult = executeLocal

executeLocal :: PCollection b p a -> ExceptT (LocalIOError String) IO [a]
executeLocal (pcoll :> ptrans) = executeLocal pcoll >>= (return . executeTransformPure ptrans)
executeLocal (pcoll :- sink) = sinkFile pcoll >> executeLocal pcoll
executeLocal (_ :+ src) =
  case src of Empty -> return []
              CreateBounded i l -> return $ take i l
              CreateUnbounded l -> return l
              SourceFile path f -> undefined -- TODO

sinkFile :: PCollection b p a -> ExceptT (LocalIOError String) IO ()
sinkFile = undefined
