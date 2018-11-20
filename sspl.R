
# Shut the imports up!
devnull <- file('/dev/null','w')

sink(file = devnull,type = 'message')

require(stringr)
require(glue)

require(tools)

require(jsonlite)
require(redux)

require(reticulate)

require(AgnusGlareTool)
require(ComfyInTurns)
require(DBgratia)

# Utility stuff ####################

scriptpath <- ComfyInTurns::myPath()

config <- readLines('stdin')%>%
	fromJSON()

chunksize <- config$chunksize
key <- config$redis$listkey

sink(type = 'message')

# Reticulate stuff #################

use_virtualenv('dfi')
nltk <- import('nltk')
tokenize <- nltk$sent_tokenize

# Redis stuff ######################

sink(devnull)
redis_config(host = config$redis$hostname,
	     port = config$redis$port,
	     db = config$redis$db)
sink()

redis <- hiredis()

# Process data #####################

strWrap <- function(df,col,SPLFUN){
	AgnusGlareTool::spreadTextRows(df,col,SPLFUN)
	}

DBgratia::redisChunkApply(redis,
			  key,
			  FUN = strWrap,
			  col = 'body',
			  SPLFUN = tokenize,
			  chunksize = chunksize,
			  verbose = TRUE)

