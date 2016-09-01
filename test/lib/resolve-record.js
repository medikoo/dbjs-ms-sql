'use strict';

module.exports = function (t, a) {
	a.deep(t({
		OwnerId: 'aaa',
		Path: 'bbb',
		Stamp: 23424,
		ValueType: 2,
		ValueBoolan: null,
		ValueNumber: 5454,
		ValueString: null,
		ValueDateTime: null,
		ValueRegExp: null,
		ValueFunction: null,
		ValueObject: null
	}), {
		id: 'aaa/bbb',
		ownerId: 'aaa',
		path: 'bbb',
		stamp: 23424,
		value: '25454'
	});
};
