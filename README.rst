Krakenous
=========

Krakenous is a Python "backend" for machine-learning tasks. It provides various storage options (currently,
SQLite and Python shelves are implemented), and a lot of helper methods/functions. Designed to help with Kaggle
competitions.

Why?
----

Because time spent on organizing data loading/storage can be spent doing more interesting things (like
writing complex functions to classificate octopii based on photos of their tentacles). You write the
data/feature-extracting functions, Krakenous takes care of writing them to disk.

Why "Krakenous"? Krakens are awesome, and "krakenous" would make one hell of an adjective.

What can it do?
---------------

Store data. Extract features and store them (you'll have to write the extractor function yourself).
You can call do something like:

.. sourcecode::python

    mydataset.extract_feature_simple(some_data_feature_extractor_function)

and it will extract and store the feature for the whole dataset. It comes with helper functions to build base
datasets from CSV files, files in folders (for example - images in folders), convert things into numpy arrays.
You can work as a team, too - Krakenous supports basic merging of different datasets (Alice extracts some of the features,
Bob extracts some other features, they combine their datasets and save time).

It is modular - you can plug in your own serializers, functions to create the initial dataset (search for files in folders,
parse CSV, whatever you need). Even writing your own backend is not that hard (probably).

What will it be able to do?
---------------------------

Support more backend types, export to CSV.