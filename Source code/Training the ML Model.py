from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

#convertion transaction data into features
assembler = VectorAssembler(inputCols=["amount"], outputCol="features")
rf = RandomForestClassifier(labelCol="fraud_label", featuresCol="features")
pipeline = Pipeline(stages=[assembler, rf])

#Train model on historical transaction Data
model = pipeline.fit(training_data)