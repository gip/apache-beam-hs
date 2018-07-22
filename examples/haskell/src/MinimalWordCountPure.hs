
{-# LANGUAGE TypeInType #-}
module MinimalWordCountPure (run) where

import Data.List (intercalate)
import Text.Regex.Posix ((=~))
import Control.Monad.Identity (runIdentity, join)
import Control.Monad.Except (runExceptT)
import Database.Apache.Beam
import Database.Apache.Beam.Executor.Pure

minimalWordCount p input =
  p :+ CreateUnbounded input
    :> FlatMap (\x -> join ((x :: String) =~ "[A-Za-z\']+" :: [[String]]))
    :> Map (\x -> (x, 1))
    :> CombinePerKey (\k l -> length l)
    :> Map (\(k,v) -> concat [k, ": ", show v])

run =
  case result of
    Left () -> error "The impossible happened: pure pipeline failure"
    Right r -> do
      putStrLn $ concat [ "Let's run minimalWordCount on the following text:\n\n",
                          staticQuote,
                          "\n\nResults are:",
                          intercalate "  " r ]
  where
    pipe = purePipeline
    result = runIdentity $ runExceptT $ executeWithResult $ minimalWordCount pipe [staticQuote]

staticQuote =
   "So shaken as we are, so wan with care, \
   \Find we a time for frighted peace to pant, \
   \And breathe short-winded accents of new broils \
   \To be commenced in strands afar remote. \
   \No more the thirsty entrance of this soil \
   \Shall daub her lips with her own children's blood; \
   \Nor more shall trenching war channel her fields, \
   \Nor bruise her flowerets with the armed hoofs \
   \Of hostile paces: those opposed eyes, \
   \Which, like the meteors of a troubled heaven, \
   \All of one nature, of one substance bred, \
   \Did lately meet in the intestine shock \
   \And furious close of civil butchery \
   \Shall now, in mutual well-beseeming ranks, \
   \March all one way and be no more opposed \
   \Against acquaintance, kindred and allies: \
   \The edge of war, like an ill-sheathed knife, \
   \No more shall cut his master. Therefore, friends, \
   \As far as to the sepulchre of Christ, \
   \Whose soldier now, under whose blessed cross \
   \We are impressed and engaged to fight, \
   \Forthwith a power of English shall we levy; \
   \Whose arms were moulded in their mothers' womb \
   \To chase these pagans in those holy fields \
   \Over whose acres walk'd those blessed feet \
   \Which fourteen hundred years ago were nail'd \
   \For our advantage on the bitter cross. \
   \But this our purpose now is twelve month old, \
   \And bootless 'tis to tell you we will go: \
   \Therefore we meet not now. Then let me hear \
   \Of you, my gentle cousin Westmoreland, \
   \What yesternight our council did decree \
   \In forwarding this dear expedience."
