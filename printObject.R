################################################################################
### Print function
##
##
##
## Simo Goshev
## Oct 03, 2019


print2File <- function(myobj, fname, df = TRUE) {
    myf <- file(fname, open="wt")
    sink(file = myf)
    if (df) {
        print(head(myobj, nrow(myobj)))
    } else {
        print(myobj)
    }
	sink(type = "message")
    sink()
    close(myf)
}

