#!/usr/bin/env Rscript

# Download data from URL
data <- read.csv("https://open-covid-19.github.io/data/data.csv")

# Rename columns to lowercase
names(data) <- tolower(names(data))

# Output the first 10 rows of data as-is
write.csv(data[1:10,], stdout())
