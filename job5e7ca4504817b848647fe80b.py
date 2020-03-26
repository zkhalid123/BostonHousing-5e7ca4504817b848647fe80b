import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e7ca4504817b848647fe80c','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	BostonHousing_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e7ca4504817b848647fe80c", spark, "{'url': '/Demo/BostonTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi44999843da7d3a23cf90fd336c0bc37b', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e7ca4504817b848647fe80c','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7ca4504817b848647fe80c','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7ca4504817b848647fe80d','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	BostonHousing_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e7ca4504817b848647fe80c"],{"5e7ca4504817b848647fe80c": BostonHousing_DBFS}, "5e7ca4504817b848647fe80d", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "CRIM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "4.52", "stddev": "13.45", "min": "0.01301", "max": "88.9762", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "ZN", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "8.86", "stddev": "18.55", "min": "0.0", "max": "100.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "INDUS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "10.47", "stddev": "6.03", "min": "1.32", "max": "19.58", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "CHAS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "0.04", "stddev": "0.2", "min": "0.0", "max": "1.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "NOX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "0.55", "stddev": "0.12", "min": "0.405", "max": "0.871", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "RM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "6.36", "stddev": "0.77", "min": "4.926", "max": "8.725", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "AGE", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "69.83", "stddev": "26.4", "min": "6.5", "max": "100.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "DIS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "3.94", "stddev": "2.16", "min": "1.4165", "max": "8.9067", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "RAD", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "8.98", "stddev": "8.11", "min": "1.0", "max": "24.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "TAX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "397.92", "stddev": "152.65", "min": "193.0", "max": "666.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "PTRATIO", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "18.09", "stddev": "2.06", "min": "14.7", "max": "21.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "B", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "351.22", "stddev": "102.24", "min": "0.32", "max": "396.9", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "LSTAT", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "12.4", "stddev": "6.99", "min": "2.98", "max": "31.99", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "MEDV", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "51", "mean": "21.9", "stddev": "9.79", "min": "7.4", "max": "50.0", "missing": "0"}, "transformation": ""}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e7ca4504817b848647fe80d','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7ca4504817b848647fe80d','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7ca4504817b848647fe80e','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	BostonHousing_AutoML = tpot_execution.Tpot_execution.run(["5e7ca4504817b848647fe80d"],{"5e7ca4504817b848647fe80d": BostonHousing_AutoFE}, "5e7ca4504817b848647fe80e", spark,json.dumps( {"model_type": "classification", "label": "MEDV", "features": ["CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "none", "run_id": "", "ProjectName": "ML Sample Problems", "PipelineName": "BostonHousing", "pipelineId": "5e7ca4504817b848647fe80b", "userid": "5e58ebb7957f3f13254389b5", "runid": "", "url_ResultView": "http://23.99.85.149:3200", "experiment_id": "2341748169460103"}))

	PipelineNotification.PipelineNotification().completed_notification('5e7ca4504817b848647fe80e','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7ca4504817b848647fe80e','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)

