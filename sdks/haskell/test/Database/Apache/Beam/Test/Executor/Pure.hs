{-# LANGUAGE DataKinds, TypeSynonymInstances, MultiParamTypeClasses, GADTs #-}
module Database.Apache.Beam.Test.Executor.Pure where

import Control.Monad.Identity
import Test.Tasty
import Test.Tasty.HUnit

import Database.Apache.Beam
import Database.Apache.Beam.Executor.Pure

p :: Pipeline PurePipelineError Identity True False PurePipeline
p = runIdentity $ newPipeline

tests :: TestTree
tests = testGroup "Pure Executor"
                    [ simpleTransforms ]

simpleTransforms :: TestTree
simpleTransforms = testGroup "Simple transforms"
  [
    testCase "ParDo" $ executePure (p :+ CreateUnbounded [1, 2, 3] :> ParDo (+1))
                       @?= [2, 3, 4]
  ]
