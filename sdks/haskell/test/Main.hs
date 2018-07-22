module Main where

import Test.Tasty
import qualified Database.Apache.Beam.Test.Executor.Pure as Pure

import Test.Tasty

main :: IO ()
main = defaultMain (testGroup "Executor"
                              [ Pure.tests ])
