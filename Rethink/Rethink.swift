/**  Rethink.swift
Copyright (c) 2015 Pixelspark
Author: Tommy van der Vorst (tommy@pixelspark.nl)

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE. **/
import Foundation

public typealias ReDocument = [String: AnyObject]

public class R {
	public static func connect(url: NSURL, callback: (String?) -> ()) -> ReConnection {
		let c = ReConnection(url: url)
		c.connect(callback)
		return c
	}

	public static func uuid() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.UUID.rawValue])
	}

	public static func db(name: String) -> ReQueryDatabase {
		return ReQueryDatabase(name: name)
	}

	public static func dbCreate(name: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.DB_CREATE.rawValue, [name]])
	}

	public static func dbDrop(name: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.DB_DROP.rawValue, [name]])
	}

	public static func dbList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.DB_LIST.rawValue,])
	}

	public static func point(longitude: Double, latitude: Double) -> ReQueryPoint {
		return ReQueryPoint(longitude: longitude, latitude:latitude)
	}

	public static func expr(string: String) -> ReQueryValue {
		return ReDatum(string: string)
	}

	public static func expr(double: Double) -> ReQueryValue {
		return ReDatum(double: double)
	}

	public static func expr(binary: NSData) -> ReQueryValue {
		return ReDatum(data: binary)
	}

	public static func expr(date: NSDate) -> ReQueryValue {
		return ReDatum(date: date)
	}

	public static func expr(bool: Bool) -> ReQueryValue {
		return ReDatum(bool: bool)
	}

	public static func expr(array: [ReQueryValue]) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.MAKE_ARRAY.rawValue, array.map { return $0.jsonSerialization }])
	}

	public static func not(value: ReQueryValue) -> ReQueryValue {
		return value.not()
	}

	public static func random(lower: Int, _ upperOpen: Int) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.RANDOM.rawValue, [lower, upperOpen]])
	}

	public static func random(lower: Double, _ upperOpen: Double) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.RANDOM.rawValue, [lower, upperOpen], ["float": true]])
	}

	public static func random() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.RANDOM.rawValue, []])
	}

	public static func round(value: ReQueryValue) -> ReQueryValue {
		return value.round()
	}

	public static func ceil(value: ReQueryValue) -> ReQueryValue {
		return value.ceil()
	}

	public static func floor(value: ReQueryValue) -> ReQueryValue {
		return value.floor()
	}

	public static func now() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.NOW.rawValue])
	}

	public static func ISO8601(date: String) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.ISO8601.rawValue, [date]])
	}

	public static func error(message: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.ERROR.rawValue, [message]])
	}

	public static func branch(test: ReQuery, ifTrue: ReQuery, ifFalse: ReQuery) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.BRANCH.rawValue, [test.jsonSerialization, ifTrue.jsonSerialization, ifFalse.jsonSerialization]])
	}

	public static func range(start: ReQueryValue, end: ReQueryValue) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.RANGE.rawValue, [start, end]])
	}

	public static func js(source: String) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.JAVASCRIPT.rawValue, [source]])
	}

	public static func json(source: String) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.JSON.rawValue, [source]])
	}
}

public class ReQuery {
	public typealias Callback = (ReResponse) -> ()

	private var jsonSerialization: AnyObject

	private init(jsonSerialization: AnyObject) {
		self.jsonSerialization = jsonSerialization
	}

	public func run(connection: ReConnection, callback: Callback) {
		let query: [AnyObject] = [ReQueryType.START.rawValue, self.jsonSerialization];

		do {
			let json = try NSJSONSerialization.dataWithJSONObject(query, options: [])
			//let ss = NSString(data: json, encoding: NSUTF8StringEncoding)!
			//print("JSON=\(ss)")
			try connection.startQuery(json, callback: callback)
		}
		catch {
			callback(ReResponse.Error("An unknown error occurred"))
		}
	}

	public func typeOf() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.TYPE_OF.rawValue, [self.jsonSerialization]])
	}

	public func info() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.INFO.rawValue, [self.jsonSerialization]])
	}
}

public class ReQueryValue: ReQuery {
	public func add(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.ADD.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func sub(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.SUB.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func mul(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.MUL.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func div(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.DIV.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func mod(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.MOD.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func and(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.AND.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func or(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.OR.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func eq(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.EQ.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func ne(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.NE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func gt(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.GT.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func ge(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.GE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func lt(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.LT.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func le(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.LE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func not() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.NOT.rawValue, [self.jsonSerialization]])
	}

	public func round() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.ROUND.rawValue, [self.jsonSerialization]])
	}

	public func ceil() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.CEIL.rawValue, [self.jsonSerialization]])
	}

	public func floor() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.FLOOR.rawValue, [self.jsonSerialization]])
	}

	public func toEpochTime() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.TO_EPOCH_TIME.rawValue, [self.jsonSerialization]])
	}

	public func defaults(value: ReQueryValue) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.DEFAULT.rawValue, [self.jsonSerialization, value]])
	}

	public func toJSON() -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.TO_JSON_STRING.rawValue, [self.jsonSerialization]])
	}

	public func toJsonString() -> ReQueryValue {
		return self.toJSON()
	}

	public subscript(key: String) -> ReQueryValue {
		return ReQueryValue(jsonSerialization: [ReTerm.BRACKET.rawValue, [self.jsonSerialization, key]])
	}
}

public class ReDatum: ReQueryValue {
	private static let reqlTypeTime = "TIME"
	private static let reqlTypeBinary = "BINARY"
	private static let reqlSpecialKey = "$reql_type$"

	private init(string: String) {
		super.init(jsonSerialization: string)
	}

	private init(double: Double) {
		super.init(jsonSerialization: double)
	}

	private init(bool: Bool) {
		super.init(jsonSerialization: bool)
	}

	private init(array: [ReQueryValue]) {
		super.init(jsonSerialization: [ReTerm.MAKE_ARRAY.rawValue, array.map { return $0.jsonSerialization }])
	}

	private init(date: NSDate) {
		super.init(jsonSerialization: [ReDatum.reqlSpecialKey: ReDatum.reqlTypeTime, "epoch_time": date.timeIntervalSince1970, "timezone": "+00:00"])
	}

	private init(data: NSData) {
		super.init(jsonSerialization: [ReDatum.reqlSpecialKey: ReDatum.reqlTypeBinary, "data": data.base64EncodedStringWithOptions([])])
	}

	private override init(jsonSerialization: AnyObject) {
		super.init(jsonSerialization: jsonSerialization)
	}

	private var value: AnyObject { get {
		if let d = self.jsonSerialization as? [String: AnyObject], let t = d[ReDatum.reqlSpecialKey] as? String {
			if t == ReDatum.reqlTypeBinary {
				if let data = self.jsonSerialization.valueForKey("data") as? String {
					return NSData(base64EncodedString: data, options: [])!
				}
				else {
					fatalError("invalid binary datum received")
				}
			}
			else if t == ReDatum.reqlTypeTime {
				if let epochTime = self.jsonSerialization.valueForKey("epoch_time"), let timezone = self.jsonSerialization.valueForKey("timezone") as? String {
					// TODO: interpret server timezone other than +00:00 (UTC)
					assert(timezone == "+00:00", "support for timezones other than UTC not implemented (yet)")
					return NSDate(timeIntervalSince1970: epochTime.doubleValue!)
				}
				else {
					fatalError("invalid date received")
				}
			}
			else {
				fatalError("unrecognized $reql_type$ in serialized data")
			}
		}
		else {
			return self.jsonSerialization
		}
	} }
}

public func +(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.add(rhs)
}

public func -(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.sub(rhs)
}

public func *(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.mul(rhs)
}

public func /(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.div(rhs)
}

public func %(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.mod(rhs)
}

public func &&(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.and(rhs)
}

public func ||(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.or(rhs)
}

public func ==(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.eq(rhs)
}

public func !=(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.ne(rhs)
}

public func >(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.gt(rhs)
}

public func >=(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.ge(rhs)
}

public func <(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.lt(rhs)
}

public func <=(lhs: ReQueryValue, rhs: ReQueryValue) -> ReQueryValue {
	return lhs.le(rhs)
}

public prefix func !(lhs: ReQueryValue) -> ReQueryValue {
	return lhs.not()
}

public typealias RePredicate = (ReQueryValue) -> (ReQuery)

public class ReQueryLambda: ReQuery {
	private static var parameterCounter = 0

	init(block: RePredicate) {
		let parameter = ReQueryValue(jsonSerialization: ++ReQueryLambda.parameterCounter)
		let parameterAccess = ReQueryValue(jsonSerialization: [ReTerm.VAR.rawValue, [parameter.jsonSerialization]])

		super.init(jsonSerialization: [
				ReTerm.FUNC.rawValue, [
						[ReTerm.MAKE_ARRAY.rawValue, [parameter.jsonSerialization]],
						block(parameterAccess).jsonSerialization
				]
		])
	}
}

public class ReQueryPoint: ReQueryValue {
	init(longitude: Double, latitude: Double) {
		super.init(jsonSerialization: [ReTerm.POINT.rawValue, [longitude, latitude]])
	}
}

public class ReQueryDatabase: ReQuery {
	private init(name: String) {
		super.init(jsonSerialization: [ReTerm.DB.rawValue, [name]])
	}

	public func table(name: String) -> ReQueryTable {
		return ReQueryTable(database: self, name: name)
	}

	public func tableCreate(name: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.TABLE_CREATE.rawValue, [self.jsonSerialization, name]])
	}

	public func tableDrop(name: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.TABLE_DROP.rawValue, [self.jsonSerialization, name]])
	}

	public func tableList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.TABLE_LIST.rawValue])
	}

	public func wait() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.WAIT.rawValue, [self.jsonSerialization]])
	}

	public func rebalance() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.REBALANCE.rawValue, [self.jsonSerialization]])
	}
}

public class ReQuerySequence: ReQuery {
	public func count() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.COUNT.rawValue, [self.jsonSerialization]])
	}

	public func limit(count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.LIMIT.rawValue, [self.jsonSerialization, count]])
	}

	public func skip(count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.SKIP.rawValue, [self.jsonSerialization, count]])
	}

	public func nth(index: Int) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.NTH.rawValue, [self.jsonSerialization, index]])
	}

	public func isEmpty() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.IS_EMPTY.rawValue, [self.jsonSerialization]])
	}

	public func filter(specification: [String: ReQueryValue]) -> ReQuerySequence {
		var serialized: [String: AnyObject] = [:]
		for (k, v) in specification {
			serialized[k] = v.jsonSerialization
		}
		return ReQuerySequence(jsonSerialization: [ReTerm.FILTER.rawValue, [self.jsonSerialization, serialized]])
	}

	public func filter(predicate: RePredicate) -> ReQuerySequence {
		let fun = ReQueryLambda(block: predicate)
		return ReQuerySequence(jsonSerialization: [ReTerm.FILTER.rawValue, [self.jsonSerialization, fun.jsonSerialization]])
	}

	public func forEach(block: RePredicate) -> ReQuerySequence {
		let fun = ReQueryLambda(block: block)
		return ReQuerySequence(jsonSerialization: [ReTerm.FOR_EACH.rawValue, [self.jsonSerialization, fun.jsonSerialization]])
	}
}

public class ReQuerySelection: ReQuerySequence {
}

public class ReQueryStream: ReQuerySequence {
	public func changes() -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.CHANGES.rawValue, [self.jsonSerialization]])
	}

	public func zip() -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.ZIP.rawValue, [self.jsonSerialization]])
	}

	public func sample(count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.SAMPLE.rawValue, [self.jsonSerialization, count]])
	}
}

public class ReQueryTable: ReQuerySequence {
	private init(database: ReQueryDatabase, name: String) {
		super.init(jsonSerialization: [ReTerm.TABLE.rawValue, [database.jsonSerialization, name]])
	}

	public func insert(documents: [ReDocument]) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.INSERT.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, documents]]])
	}

	public func indexWait() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.INDEX_WAIT.rawValue, [self.jsonSerialization]])
	}

	public func indexDrop(name: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.INDEX_DROP.rawValue, [self.jsonSerialization, name]])
	}

	public func indexList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.INDEX_LIST.rawValue, [self.jsonSerialization]])
	}

	public func indexRename(renameIndex: String, to: String) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.INDEX_RENAME.rawValue, [self.jsonSerialization, renameIndex, to]])
	}

	public func indexStatus() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.INDEX_STATUS.rawValue, [self.jsonSerialization]])
	}

	public func status() -> ReQuerySelection {
		return ReQuerySelection(jsonSerialization: [ReTerm.STATUS.rawValue, [self.jsonSerialization]])
	}

	public func sync() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.SYNC.rawValue, [self.jsonSerialization]])
	}

	public func wait() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.WAIT.rawValue, [self.jsonSerialization]])
	}

	public func get(primaryKey: AnyObject) -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.GET.rawValue, [self.jsonSerialization, primaryKey]])
	}

	public func rebalance() -> ReQuery {
		return ReQuery(jsonSerialization: [ReTerm.REBALANCE.rawValue, [self.jsonSerialization]])
	}

	public func between(lower: ReQueryValue, _ upper: ReQueryValue) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.BETWEEN.rawValue, [self.jsonSerialization, lower.jsonSerialization, upper.jsonSerialization]])
	}
}

private enum ReQueryType: Int {
	case START =  1
	case CONTINUE = 2
	case STOP =  3
	case NOREPLY_WAIT = 4
}

private enum ReTerm: Int {
	case DATUM = 1
	case MAKE_ARRAY = 2
	case MAKE_OBJ = 3
	case VAR = 10
	case JAVASCRIPT = 11
	case UUID = 169
	case HTTP = 153
	case ERROR = 12
	case IMPLICIT_VAR = 13
	case DB = 14
	case TABLE = 15
	case GET = 16
	case GET_ALL = 78
	case EQ = 17
	case NE = 18
	case LT = 19
	case LE = 20
	case GT = 21
	case GE = 22
	case NOT = 23
	case ADD = 24
	case SUB = 25
	case MUL = 26
	case DIV = 27
	case MOD = 28
	case FLOOR = 183
	case CEIL = 184
	case ROUND = 185
	case APPEND = 29
	case PREPEND = 80
	case DIFFERENCE = 95
	case SET_INSERT = 88
	case SET_INTERSECTION = 89
	case SET_UNION = 90
	case SET_DIFFERENCE = 91
	case SLICE = 30
	case SKIP = 70
	case LIMIT = 71
	case OFFSETS_OF = 87
	case CONTAINS = 93
	case GET_FIELD = 31
	case KEYS = 94
	case OBJECT = 143
	case HAS_FIELDS = 32
	case WITH_FIELDS = 96
	case PLUCK = 33
	case WITHOUT = 34
	case MERGE = 35
	case BETWEEN_DEPRECATED = 36
	case BETWEEN = 182
	case REDUCE = 37
	case MAP = 38
	case FILTER = 39
	case CONCAT_MAP = 40
	case ORDER_BY = 41
	case DISTINCT = 42
	case COUNT = 43
	case IS_EMPTY = 86
	case UNION = 44
	case NTH = 45
	case BRACKET = 170
	case INNER_JOIN = 48
	case OUTER_JOIN = 49
	case EQ_JOIN = 50
	case ZIP = 72
	case RANGE = 173
	case INSERT_AT = 82
	case DELETE_AT = 83
	case CHANGE_AT = 84
	case SPLICE_AT = 85
	case COERCE_TO = 51
	case TYPE_OF = 52
	case UPDATE = 53
	case DELETE = 54
	case REPLACE = 55
	case INSERT = 56
	case DB_CREATE = 57
	case DB_DROP = 58
	case DB_LIST = 59
	case TABLE_CREATE = 60
	case TABLE_DROP = 61
	case TABLE_LIST = 62
	case CONFIG = 174
	case STATUS = 175
	case WAIT = 177
	case RECONFIGURE = 176
	case REBALANCE = 179
	case SYNC = 138
	case INDEX_CREATE = 75
	case INDEX_DROP = 76
	case INDEX_LIST = 77
	case INDEX_STATUS = 139
	case INDEX_WAIT = 140
	case INDEX_RENAME = 156
	case FUNCALL = 64
	case BRANCH = 65
	case OR = 66
	case AND = 67
	case FOR_EACH = 68
	case FUNC = 69
	case ASC = 73
	case DESC = 74
	case INFO = 79
	case MATCH = 97
	case UPCASE = 141
	case DOWNCASE = 142
	case SAMPLE = 81
	case DEFAULT = 92
	case JSON = 98
	case TO_JSON_STRING = 172
	case ISO8601 = 99
	case TO_ISO8601 = 100
	case EPOCH_TIME = 101
	case TO_EPOCH_TIME = 102
	case NOW = 103
	case IN_TIMEZONE = 104
	case DURING = 105
	case DATE = 106
	case TIME_OF_DAY = 126
	case TIMEZONE = 127
	case YEAR = 128
	case MONTH = 129
	case DAY = 130
	case DAY_OF_WEEK = 131
	case DAY_OF_YEAR = 132
	case HOURS = 133
	case MINUTES = 134
	case SECONDS = 135
	case TIME = 136
	case MONDAY = 107
	case TUESDAY = 108
	case WEDNESDAY = 109
	case THURSDAY = 110
	case FRIDAY = 111
	case SATURDAY = 112
	case SUNDAY = 113
	case JANUARY = 114
	case FEBRUARY = 115
	case MARCH = 116
	case APRIL = 117
	case MAY = 118
	case JUNE = 119
	case JULY = 120
	case AUGUST = 121
	case SEPTEMBER = 122
	case OCTOBER = 123
	case NOVEMBER = 124
	case DECEMBER = 125
	case LITERAL = 137
	case GROUP = 144
	case SUM = 145
	case AVG = 146
	case MIN = 147
	case MAX = 148
	case SPLIT = 149
	case UNGROUP = 150
	case RANDOM = 151
	case CHANGES = 152
	case ARGS = 154
	case BINARY = 155
	case GEOJSON = 157
	case TO_GEOJSON = 158
	case POINT = 159
	case LINE = 160
	case POLYGON = 161
	case DISTANCE = 162
	case INTERSECTS = 163
	case INCLUDES = 164
	case CIRCLE = 165
	case GET_INTERSECTING = 166
	case FILL = 167
	case GET_NEAREST = 168
	case POLYGON_SUB = 171
	case MINVAL = 180
	case MAXVAL = 181
}

/** Protocol constants. */
private class ReProtocol {
	static let defaultPort = 28015

	static let protocolVersion = 0x400c2d20 // V0_4
	static let protocolType: UInt32 = 0x7e6970c7 // JSON
	static let handshakeSuccessResponse = "SUCCESS"

	static let responseTypeSuccessAtom = 1
	static let responseTypeSuccessSequence = 2
	static let responseTypeSuccessPartial = 3
	static let responseTypeWaitComplete = 4
	static let responseTypeClientError  = 16
	static let responseTypeCompileError = 17
	static let responseTypeRuntimeError = 18
}

private extension NSData {
	static func dataWithLittleEndianOf(nr: UInt64) -> NSData {
		var swapped = CFSwapInt64HostToLittle(nr)

		var bytes: [UInt8] = [0,0,0,0,0,0,0,0]
		for(var i = 0; i <= 7; i++) {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return NSData(bytes: bytes, length: 8)
	}

	static func dataWithLittleEndianOf(nr: UInt32) -> NSData {
		var swapped = CFSwapInt32HostToLittle(nr) // No-op on little endian archs

		var bytes: [UInt8] = [0,0,0,0]
		for(var i = 0; i <= 3; i++) {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return NSData(bytes: bytes, length: 4)
	}

	func readLittleEndianUInt64(atIndex: Int = 0) -> UInt64 {
		assert(self.length >= atIndex + 8)
		let buffer = UnsafeMutablePointer<UInt8>(self.bytes)
		var read: UInt64 = 0
		for(var i = 7; i >= 0; i--) {
			read = (read << 8) + UInt64(buffer[atIndex + i])
		}
		return CFSwapInt64LittleToHost(read)
	}

	func readLittleEndianUInt32(atIndex: Int = 0) -> UInt32 {
		assert(self.length >= (atIndex + 4))
		let buffer = UnsafeMutablePointer<UInt8>(self.bytes)
		var read: UInt32 = 0
		for(var i = 3; i >= 0; i--) {
			read = (read << 8) + UInt32(buffer[atIndex + i])
		}
		return CFSwapInt32LittleToHost(read)
	}
}

private enum ReConnectionState {
	case Unconnected // Nothing has been done yet
	case HandshakeSent // Our handshake has been sent, we are waiting for confirmation of success
	case Connected // Handshake has been completed, and we are awaiting respones from the server
	case Receiving(UInt64, UInt32) // Server is sending us data for the query (Query token, response length)
	case Error(ReError) // A protocol error has occurred
	case Terminated // The connection has been terminated

	var connected: Bool { get {
		switch self {
			case .Connected, .Receiving(_, _):
				return true

			default:
				return false
		}
	} }

	var error: ReError? { get {
		switch self {
			case .Error(let e):
				return e

			default:
				return nil
		}
	} }

	mutating func onReceive(database: ReConnection) {
		switch self {
			case .Unconnected:
				// Start authentication
				guard let data = NSMutableData(capacity: 128) else {
					self = .Error(ReError.Fatal("Could not create data object"));
					return
				}

				// Append protocol version
				data.appendData(NSData.dataWithLittleEndianOf(UInt32(ReProtocol.protocolVersion)))

				// Append authentication key length and the key itself (as ASCII)
				if let authKey = database.authenticationKey?.dataUsingEncoding(NSASCIIStringEncoding) {
					data.appendData(NSData.dataWithLittleEndianOf(UInt32(authKey.length)))
					data.appendData(authKey)
				}
				else {
					data.appendData(NSData.dataWithLittleEndianOf(UInt32(0)))
				}

				// Append protocol type (JSON)
				data.appendData(NSData.dataWithLittleEndianOf(UInt32(ReProtocol.protocolType)))

				do {
					try database.outStream.send(data)
					self = .HandshakeSent
				}
				catch {
					self = .Error(ReError.Fatal("Could not send data"))
					return
				}

			case .HandshakeSent:
				if let s = database.inStream.consumeZeroTerminatedASCII() {
					if s == ReProtocol.handshakeSuccessResponse {
						self = .Connected
					}
					else {
						self = .Error(ReError.Fatal("Handshake failed, server returned: \(s)"))
					}
				}
				break

			case .Connected:
				if let d = database.inStream.consumeData(8 + 4) {
					let queryToken = d.readLittleEndianUInt64(0)
					let responseSize = d.readLittleEndianUInt32(8)
					self = .Receiving(queryToken, responseSize)
					self.onReceive(database)
				}
				break

			case .Receiving(let queryToken, let size):
				if database.inStream.buffer.length >= Int(size) {
					let d = database.inStream.consumeData(Int(size))!
					assert(d.length == Int(size))
					self = .Connected
					let continuation: ReResponse.ContinuationCallback = { (callback: ReResponse.Callback) -> () in
						do {
							try database.sendContinuation(queryToken, callback: callback)
						}
						catch {
							callback(ReResponse.Error("Could not send continuation"))
						}
					}

					if let handler = database.outstandingQueries[queryToken], let response = ReResponse(json: d, continuation: continuation) {
						handler(response)
					}
					else {
						self = .Error(ReError.Fatal("No response handler or invalid response object from server"))
					}
					self.onReceive(database)
				}
				else {
					print("Need more data for qt=\(queryToken)")
				}
				break

			case .Terminated:
				break

			case .Error:
				break
		}
	}
}

public enum ReError: ErrorType {
	case Fatal(String)
	case Other(ErrorType)
}

private class ReInputStream: NSObject, NSStreamDelegate {
	typealias Callback = (ReInputStream) -> ()
	let stream: NSInputStream
	private var error: ReError? = nil
	private var buffer: NSMutableData = NSMutableData(capacity: 512)!
	var readCallback: Callback? = nil

	init(stream: NSInputStream) {
		self.stream = stream
		super.init()
		self.stream.delegate = self
	}

	private func drainStream() throws {
		while self.stream.hasBytesAvailable {
			let buffer = NSMutableData(length: 512)!
			let read = self.stream.read(UnsafeMutablePointer<UInt8>(buffer.mutableBytes), maxLength: buffer.length)
			if read > 0 {
				self.buffer.appendBytes(buffer.bytes, length: read)
			}
			else {
				try checkError()
			}
		}
	}

	private func consumeData(length: Int) -> NSData? {
		assert(length>0)
		if self.buffer.length >= length {
			let sliced = NSData(bytes: self.buffer.bytes, length: length)
			self.buffer = NSMutableData(bytes: self.buffer.bytes.advancedBy(length), length: self.buffer.length - length)
			return sliced
		}
		return nil
	}

	private func consumeZeroTerminatedASCII() -> String? {
		let bytes = UnsafePointer<UInt8>(self.buffer.bytes)
		for(var i=0; i<self.buffer.length; i++) {
			if bytes[i] == 0 {
				if let x = NSString(bytes: bytes, length: i, encoding: NSASCIIStringEncoding) {
					self.buffer = NSMutableData(bytes: self.buffer.bytes.advancedBy(i+1), length: self.buffer.length - i - 1)
					return String(x)
				}
				return nil
			}
		}
		return nil
	}

	private func checkError() throws {
		if let e = self.error {
			throw e
		}
		if let e = self.stream.streamError {
			throw ReError.Other(e)
		}
	}

	@objc private func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
		do {
			switch eventCode {
				case NSStreamEvent.HasBytesAvailable:
					try self.drainStream()
					self.readCallback?(self)

				case NSStreamEvent.OpenCompleted:
					break

				case NSStreamEvent.ErrorOccurred:
					try self.checkError()

				case NSStreamEvent.EndEncountered:
					break

				default:
					print("Unhandled input stream event: \(eventCode)")
			}
		}
		catch ReError.Fatal(let e) {
			self.error = ReError.Fatal(e)
		}
		catch {
			self.error = ReError.Fatal("Unknown error")
		}
	}

	deinit {
		self.stream.close()
	}
}

private class ReOutputStream: NSObject, NSStreamDelegate {
	let stream: NSOutputStream
	private var error: ReError? = nil
	private var outQueue: [NSData] = []

	init(stream: NSOutputStream) {
		self.stream = stream
		super.init()
		self.stream.delegate = self
	}

	deinit {
		self.stream.close()
	}

	private func checkError() throws {
		if let e = self.stream.streamError {
			throw ReError.Other(e)
		}
	}

	@objc private func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
		do {
			switch eventCode {
				case NSStreamEvent.OpenCompleted:
					try drainQueue()

				case NSStreamEvent.ErrorOccurred:
					try checkError()

				case NSStreamEvent.HasSpaceAvailable:
					try drainQueue()

				default:
					print("Unhandled output stream event: \(eventCode)")
					break;
			}
		}
		catch {
			self.error = ReError.Fatal("Error in drainQueue")
		}
	}

	private func send(data: NSData) throws {
		self.outQueue.append(data)
		try drainQueue()
	}

	private func drainQueue() throws {
		try checkError()
		while self.stream.hasSpaceAvailable && self.outQueue.count > 0 {
			let data = outQueue.removeFirst()
			let buffer = UnsafePointer<UInt8>(data.bytes)
			let bytesWritten = self.stream.write(buffer, maxLength: data.length)
			try checkError()

			if bytesWritten < data.length {
				let sliced = NSData(bytes: buffer.advancedBy(bytesWritten), length: data.length - bytesWritten)
				outQueue.insert(sliced, atIndex: 0)
			}
		}
	}
}

public enum ReResponse {
	public typealias Callback = (ReResponse) -> ()
	public typealias ContinuationCallback = (Callback) -> ()

	case Error(String)
	case Value(AnyObject)
	case Rows([ReDocument], ContinuationCallback?)
	case Unknown

	init?(json: NSData, continuation: ContinuationCallback) {
		do {
			if let d = try NSJSONSerialization.JSONObjectWithData(json, options: []) as? NSDictionary {
				if let type = d.valueForKey("t") as? NSNumber {
					switch type.integerValue {
						case ReProtocol.responseTypeSuccessAtom:
							guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
							if r.count != 1 { return nil }
							self = .Value(ReDatum(jsonSerialization: r.first!).value)

						case ReProtocol.responseTypeSuccessPartial, ReProtocol.responseTypeSuccessSequence:
							if let r = d.valueForKey("r") as? [ReDocument] {
								let deserialized = r.map { (document) -> ReDocument in
									var dedoc: ReDocument = [:]
									for (k, v) in document {
										dedoc[k] = ReDatum(jsonSerialization: v).value
									}
									return dedoc
								}

								self = .Rows(deserialized, type.integerValue == ReProtocol.responseTypeSuccessPartial ? continuation : nil)
							}
							else {
								return nil
							}

						case ReProtocol.responseTypeClientError:
							guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
							if r.count != 1 { return nil }
							self = .Error("Client error: \(r.first!)")

						case ReProtocol.responseTypeCompileError:
							guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
							if r.count != 1 { return nil }
							self = .Error("Compile error: \(r.first!)")

						case ReProtocol.responseTypeRuntimeError:
							guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
							if r.count != 1 { return nil }
							self = .Error("Run-time error: \(r.first!)")

						default:
							self = .Unknown
					}
				}
				else {
					return nil
				}
			}
			else {
				return nil
			}
		}
		catch {
			return nil
		}
	}

	public var isError: Bool { get {
		switch self {
			case .Error(_): return true
			default: return false
		}
	} }

	public var value: AnyObject? { get {
		switch self {
			case .Value(let v):
				return v

			default:
				return nil
		}
	} }
}

public class ReConnection: NSObject, NSStreamDelegate {
	private typealias QueryToken = UInt64
	public let url: NSURL
	public var authenticationKey: String? { get { return self.url.user } }

	private var inStream: ReInputStream! = nil
	private var outStream: ReOutputStream! = nil
	private var state: ReConnectionState = .Unconnected
	private var outstandingQueries: [QueryToken: ReResponse.Callback] = [:]

	/** Create a connection to a RethinkDB instance. The URL should be of the form 'rethinkdb://host:port'. If
	no port is given, the default port is used. If the server requires the use of an authentication key, put it
	in the 'user' part of the URL, e.g. "rethinkdb://key@server:port". */
	private init(url: NSURL) {
		self.url = url
	}

	private func connect(callback: (String?) -> ()) {
		assert(!self.state.connected)

		let port = (url.port ?? ReProtocol.defaultPort).integerValue
		assert(port < 65536)

		guard let hostname = url.host else {
			self.inStream = nil
			self.outStream = nil
			callback("Invalid URL")
			return
		}

		// Create a pair of streams to read/write to the database
		var readStream: Unmanaged<CFReadStreamRef>?
		var writeStream: Unmanaged<CFWriteStreamRef>?
		CFStreamCreatePairWithSocketToHost(nil, hostname, UInt32(port), &readStream, &writeStream)

		if let r = readStream, w = writeStream {
			let inStream: NSInputStream = r.takeRetainedValue()
			let outStream: NSOutputStream = w.takeRetainedValue()

			inStream.scheduleInRunLoop(NSRunLoop.mainRunLoop(), forMode: NSDefaultRunLoopMode)
			outStream.scheduleInRunLoop(NSRunLoop.mainRunLoop(), forMode: NSDefaultRunLoopMode)
			self.outStream = ReOutputStream(stream: outStream)
			self.inStream = ReInputStream(stream: inStream)

			var first = true

			self.inStream.readCallback = { [weak self] (inStream) in
				if let s = self {
					s.state.onReceive(s)
					if s.connected {
						if first {
							callback(nil)
							first = false
						}
					}
				}
			}
			inStream.open()
			outStream.open()
			state.onReceive(self)
		}
		else {
			inStream = nil
			outStream = nil
			callback("connection failed")
		}
	}

	public var connected: Bool { get {
		return self.state.connected
	} }

	public var error: ReError? { get {
		return self.state.error
	} }

	private var nextQueryToken: UInt64 = 0x5ADFACE

	private func sendContinuation(queryToken: QueryToken, callback: ReResponse.Callback) throws {
		let json = [ReQueryType.CONTINUE.rawValue];
		let query = try NSJSONSerialization.dataWithJSONObject(json, options: [])
		try sendQuery(query, token: queryToken, callback: callback)
	}

	private func sendQuery(query: NSData, token: QueryToken, callback: ReResponse.Callback) throws {
		assert(self.connected, "Cannot send a query when the connection is not open")
		let data = NSMutableData(capacity: query.length + 8 + 4)!
		self.outstandingQueries[token] = callback
		data.appendData(NSData.dataWithLittleEndianOf(token))
		data.appendData(NSData.dataWithLittleEndianOf(UInt32(query.length)))
		data.appendData(query)
		try self.outStream.send(data)
	}

	private func startQuery(query: NSData, callback: ReResponse.Callback) throws {
		try sendQuery(query, token: nextQueryToken, callback: callback)
		nextQueryToken++
	}
}