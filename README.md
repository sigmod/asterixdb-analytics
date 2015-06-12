AsterixDB-Analytis
========

AsterixDB-Analytics is a umbrella project for large-scale data analytical frameworks built on top of Hyracks. Currently it includes Pregelix and IMRU.

Pregelix is an open-source implementation of the bulk-synchronous vertex-oriented programming model (the Pregel API) for large-scale graph analytics. Pregelix is based on an iterative dataflow design that is better tuned to handle both in-memory and out-of-core workloads efficiently. Pregelix supports a variety of physical runtime choice which can fit different sorts of graph algorithms, datasets, and clusters. Our motto is "Big Graph Analytics Anywhere" --- from a single laptop to large enterprise clusters.
Pregelix website: http://pregelix.ics.uci.edu


IMRU is an open-source implementation of Iterative Map Reduce Update.
IMRU provides a general framework for parallelizing the class of machine-learning algorithms that correspond to the statistical query model. For instance, this class includes batch learning and Expectation Maximization. A machine learning algorithm in this class can be split into three steps:
A map step, where all training data is evaluated against the current model.
A reduce step, where the outputs of the map step are aggregated. The aggregation function is assumed to be commutative and associative.
A update step, where the model is revised based on the result of the reduce step.

