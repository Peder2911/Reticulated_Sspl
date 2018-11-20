
as.sentence.df <- function(df,col = 'body'){

	print(names(df))
	dfs <- apply(df,1,function(row){

		sentence <- row[col]%>%
			tokenizers::tokenize_sentences(simplify = TRUE)
		
		notSent <- names(row)[names(row)!=col]

		rest <- row[notSent]%>%
			lapply(rep,length(sentence))
		
		out <- data.frame(rest,sentence,stringsAsFactors = FALSE)

			})
	dfs <- dfs %>%
	bind_rows()
		}

