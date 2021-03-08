# Labelling the Pairs
## Purpose
- Create a labelled vector for a sample of pairs: "It's a Match"  (y_true = 1) or "Not a Match" (y_true = 0)
- Sample the dataset to take an informative sample of the different possibilities
    - The algorithm will not perform well if we take as supervised learning always the same common cases 
    - We need to find cases that are near the decision frontier of the algorithm

## Detailed Steps
### Part 1: Non-supervised clustering
- From the scores matrix, create a clustering, as the combination of two clustering strategies:
    - One using K-Means
    - One using K-Bins on the total score
- This is done in order to have a good balance of diversity of scores (by K-Means) and sorting by total score (by using KBins)
- It performs quite well in practice

### Part 2: Take a sample of pairs out of each cluster
- Take for each clusters 10 pairs
- Ask the user to label if this is a pair or not

### Part 3: Ask questions on the hard clusters
- Hard clusters are clusters containing both Positive and Negative matches
- They are close to the decision frontier

### Part 4: Upload the results to Postgres
- Copy the results of the labellization steps to Postgres to fill the y_true table

## Improvment / Workaround
### Use Classifiers to find pairs with low confidence
- Instead of using cluster, one can as well train a classifier, and label a sample the pairs with low-confidence (~ 0.5)
- This performs really well for selecting hard cases
