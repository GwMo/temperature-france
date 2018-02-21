# Split a data frame into a train and test set for cross-validation
split_for_cv <- function(dataframe, cv_folds = 10, seed=NULL) {
  if (!is.null(seed)) { set.seed(seed) }

  # Randomly select 1 / cv_folds of the rows (default is 1/10th)
  test_index <- sample(1:nrow(dataframe), trunc(nrow(dataframe) / cv_folds))

  list(
    trainset = dataframe[-test_index, ],
    testset = dataframe[test_index, ]
  )
}
