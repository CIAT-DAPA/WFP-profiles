f1 <- "archive/attempt_1.log"
con <- file(f1)
sink(con, append=TRUE)
sink(con, append=TRUE, type="message")


# This will echo all input and not truncate 150+ character lines...
source("archive/999_01_gcm_data_processing.R", echo=TRUE, max.deparse.length=10000)

# Restore output to console
sink() 
sink(type="message")

# And look at the log...
cat(readLines(f1), sep="\n")