from pysmartdatamodels import pysmartdatamodels as sdm

subject = "dataModel.Weather"

dataModel = "WeatherForecast"

attribute = "precipitation"

print(sdm.model_attribute(subject, dataModel, attribute))
print(sdm.datatype_attribute(subject, dataModel, attribute))

print(sdm.datamodels_subject(subject))


print(sdm.look_for_datamodel("Temperature", 84))
