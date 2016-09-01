'use strict';

module.exports = function (record) {
	var result = {
		id: record.OwnerId + (record.Path ? ('/' + record.Path) : ''),
		ownerId: record.OwnerId,
		path: record.Path,
		stamp: Number(record.Stamp) // BigInt is resolved as string
	};
	if (record.ValueType == null) {
		result.value = '';
	} else if (record.ValueType === 0) {
		result.value = '0';
	} else if (record.ValueType === 1) {
		result.value = '1' + record.ValueBoolean;
	} else if (record.ValueType === 2) {
		result.value = '2' + record.ValueNumber;
	} else if (record.ValueType === 3) {
		result.value = '3' + JSON.stringify(record.ValueString).slice(1, -1);
	} else if (record.ValueType === 4) {
		result.value = '4' + Number(record.ValueDateTime);
	} else if (record.ValueType === 5) {
		result.value = '5' + JSON.stringify(record.ValueRegExp).slice(1, -1);
	} else if (record.ValueType === 6) {
		result.value = '6' + JSON.stringify(record.ValueFunction).slice(1, -1);
	} else if (record.ValueType === 7) {
		result.value = '7' + record.ValueObject;
	} else if (record.ValueType === 8) {
		result.value = JSON.parse(record.ValueMultiple);
	}
	return result;
};
