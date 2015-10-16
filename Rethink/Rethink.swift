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
	public static func connect(url: NSURL, callback: (String?, ReConnection) -> ()) {
		let c = ReConnection(url: url)
		c.connect { (err) in
			callback(err, c)
		}
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
		let query: [AnyObject] = [ReProtocol.ReQueryType.START.rawValue, self.jsonSerialization];

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

	internal init(string: String) {
		super.init(jsonSerialization: string)
	}

	internal init(double: Double) {
		super.init(jsonSerialization: double)
	}

	internal init(bool: Bool) {
		super.init(jsonSerialization: bool)
	}

	internal init(array: [ReQueryValue]) {
		super.init(jsonSerialization: [ReTerm.MAKE_ARRAY.rawValue, array.map { return $0.jsonSerialization }])
	}

	internal init(date: NSDate) {
		super.init(jsonSerialization: [ReDatum.reqlSpecialKey: ReDatum.reqlTypeTime, "epoch_time": date.timeIntervalSince1970, "timezone": "+00:00"])
	}

	internal init(data: NSData) {
		super.init(jsonSerialization: [ReDatum.reqlSpecialKey: ReDatum.reqlTypeBinary, "data": data.base64EncodedStringWithOptions([])])
	}

	internal override init(jsonSerialization: AnyObject) {
		super.init(jsonSerialization: jsonSerialization)
	}

	internal var value: AnyObject { get {
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
		return ReQuerySequence(jsonSerialization: [ReTerm.TABLE_LIST.rawValue, [self.jsonSerialization]])
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

	public func sample(count: Int) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.SAMPLE.rawValue, [self.jsonSerialization, count]])
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

	public override func sample(count: Int) -> ReQueryStream {
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

