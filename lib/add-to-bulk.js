'use strict';

var isArray = Array.isArray;

module.exports = function (rows, isComputed, ownerId, path, data) {
	var type, value, params = {}, tokens = []
	  , record = { ownerId: ownerId, path: path, params: params, tokens: tokens };
	if (isComputed && isArray(data.value)) {
		type = 8;
		value = JSON.stringify(data.value);
	} else {
		type = data.value[0] ?  Number(data.value[0]) : null;
		value = data.value.slice(1);
	}
	tokens.push(['Stamp', String(data.stamp)]);
	tokens.push(['ValueType', String((type == null) ? 'NULL' : type)]);
	tokens.push(['ValueBoolean', String((type === 1) ? Number(value) : 'NULL')]);
	tokens.push(['ValueNumber', String((type === 2) ? Number(value) : 'NULL')]);
	if (type === 3) {
		params.string = JSON.parse('"' + value + '"');
		tokens.push(['ValueString', '@string']);
	} else {
		tokens.push(['ValueString', 'NULL']);
	}
	if (type === 4) {
		params.dateTime = new Date(Number(value));
		tokens.push(['ValueDateTime', '@dateTime']);
	} else {
		tokens.push(['ValueDateTime', 'NULL']);
	}
	if (type === 5) {
		params.regExp = JSON.parse('"' + value + '"');
		tokens.push(['ValueRegExp', '@regExp']);
	} else {
		tokens.push(['ValueRegExp', 'NULL']);
	}
	if (type === 6) {
		params.function = JSON.parse('"' + value + '"');
		tokens.push(['ValueFunction', '@function']);
	} else {
		tokens.push(['ValueFunction', 'NULL']);
	}
	if (type === 7) {
		params.object = value;
		tokens.push(['ValueObject', '@object']);
	} else {
		tokens.push(['ValueObject', 'NULL']);
	}
	if (type === 8) {
		params.multiple = value;
		tokens.push(['ValueMultiple', '@multiple']);
	} else if (isComputed) {
		tokens.push(['ValueMultiple', 'NULL']);
	}
	rows.push(record);
};
