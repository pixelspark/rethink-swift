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

public class R {
	public static func connect(url: NSURL, callback: (String?, ReConnection) -> ()) {
		let c = ReConnection(url: url)
		c.connect { (err) in
			callback(err, c)
		}
	}

	public static func uuid() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.UUID.rawValue])
	}

	public static func db(name: String) -> ReQueryDatabase {
		return ReQueryDatabase(name: name)
	}

	public static func dbCreate(name: String) -> ReQuery {
		return ReDatum(jsonSerialization: [ReTerm.DB_CREATE.rawValue, [name]])
	}

	public static func dbDrop(name: String) -> ReQuery {
		return ReDatum(jsonSerialization: [ReTerm.DB_DROP.rawValue, [name]])
	}

	public static func dbList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.DB_LIST.rawValue])
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

	public static func expr(int: Int) -> ReQueryValue {
		return ReDatum(int: int)
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

	public static func expr() -> ReQueryValue {
		return ReDatum()
	}

	public static func expr(document: ReDocument) -> ReQueryValue {
		return ReDatum(document: document)
	}

	public static func expr(object: [String: ReQueryValue]) -> ReQueryValue {
		return ReDatum(object: object)
	}

	public static func expr(array: [ReQueryValue]) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MAKE_ARRAY.rawValue, array.map { return $0.jsonSerialization }])
	}

	public static func not(value: ReQueryValue) -> ReQueryValue {
		return value.not()
	}

	public static func random(lower: Int, _ upperOpen: Int) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.RANDOM.rawValue, [lower, upperOpen]])
	}

	public static func random(lower: Double, _ upperOpen: Double) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.RANDOM.rawValue, [lower, upperOpen], ["float": true]])
	}

	public static func random() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.RANDOM.rawValue, []])
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
		return ReDatum(jsonSerialization: [ReTerm.NOW.rawValue])
	}

	public static func ISO8601(date: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ISO8601.rawValue, [date]])
	}

	public static func error(message: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ERROR.rawValue, [message]])
	}

	public static func branch(test: ReQuery, ifTrue: ReQuery, ifFalse: ReQuery) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.BRANCH.rawValue, [test.jsonSerialization, ifTrue.jsonSerialization, ifFalse.jsonSerialization]])
	}

	public static func object(key: ReQueryValue, value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.OBJECT.rawValue, [key.jsonSerialization, value.jsonSerialization]])
	}

	public static func range(start: ReQueryValue, _ end: ReQueryValue) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.RANGE.rawValue, [start.jsonSerialization, end.jsonSerialization]])
	}

	public static func js(source: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.JAVASCRIPT.rawValue, [source]])
	}

	public static func json(source: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.JSON.rawValue, [source]])
	}
}

public class ReQueryDatabase: ReQuery {
	public let jsonSerialization: AnyObject

	private init(name: String) {
		self.jsonSerialization = [ReTerm.DB.rawValue, [name]]
	}

	public func table(name: String) -> ReQueryTable {
		return ReQueryTable(database: self, name: name)
	}

	public func tableCreate(name: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TABLE_CREATE.rawValue, [self.jsonSerialization, name]])
	}

	public func tableDrop(name: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TABLE_DROP.rawValue, [self.jsonSerialization, name]])
	}

	public func tableList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.TABLE_LIST.rawValue, [self.jsonSerialization]])
	}

	public func wait() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.WAIT.rawValue, [self.jsonSerialization]])
	}

	public func rebalance() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.REBALANCE.rawValue, [self.jsonSerialization]])
	}
}

public class ReQuerySequence: ReQuery {
	public let jsonSerialization: AnyObject

	private init(jsonSerialization: AnyObject) {
		self.jsonSerialization = jsonSerialization
	}

	public func distinct() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.DISTINCT.rawValue, [self.jsonSerialization]])
	}

	public func count() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.COUNT.rawValue, [self.jsonSerialization]])
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

	public func nth(index: Int) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.NTH.rawValue, [self.jsonSerialization, index]])
	}

	public func isEmpty() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.IS_EMPTY.rawValue, [self.jsonSerialization]])
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

	public func innerJoin(foreign: ReQuerySequence, predicate: RePredicate) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.INNER_JOIN.rawValue, [self.jsonSerialization, foreign.jsonSerialization, ReQueryLambda(block: predicate).jsonSerialization]])
	}

	public func outerJoin(foreign: ReQuerySequence, predicate: RePredicate) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.OUTER_JOIN.rawValue, [self.jsonSerialization, foreign.jsonSerialization, ReQueryLambda(block: predicate).jsonSerialization]])
	}

	public func eqJoin(leftField: ReQueryValue, foreign: ReQueryTable) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.EQ_JOIN.rawValue, [self.jsonSerialization, leftField.jsonSerialization, foreign.jsonSerialization]])
	}

	public func map(mapper: ReQueryLambda) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.MAP.rawValue, [self.jsonSerialization, mapper.jsonSerialization]])
	}

	public func map(block: RePredicate) -> ReQuerySequence {
		return self.map(ReQueryLambda(block: block))
	}

	public func withFields(fields: [ReQueryValue]) -> ReQuerySequence {
		let values = fields.map({ e in return e.jsonSerialization })
		return ReQuerySequence(jsonSerialization: [ReTerm.WITH_FIELDS.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, values]]])
	}

	public func union(sequence: ReQuerySequence) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.UNION.rawValue, [self.jsonSerialization, sequence.jsonSerialization]])
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

	public func insert(documents: [ReDocument]) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INSERT.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, documents]]])
	}

	public func insert(objects: [ReQueryValue]) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INSERT.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, objects.map { return $0.jsonSerialization }]]])
	}

	public func update(changes: ReDocument) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.UPDATE.rawValue, [self.jsonSerialization, changes]])
	}

	public func replace(changes: ReDocument) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.REPLACE.rawValue, [self.jsonSerialization, changes]])
	}

	public func delete() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DELETE.rawValue, [self.jsonSerialization]])
	}

	public func indexWait() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INDEX_WAIT.rawValue, [self.jsonSerialization]])
	}

	public func indexDrop(name: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INDEX_DROP.rawValue, [self.jsonSerialization, name]])
	}

	public func indexList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.INDEX_LIST.rawValue, [self.jsonSerialization]])
	}

	public func indexRename(renameIndex: String, to: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INDEX_RENAME.rawValue, [self.jsonSerialization, renameIndex, to]])
	}

	public func indexStatus() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INDEX_STATUS.rawValue, [self.jsonSerialization]])
	}

	public func status() -> ReQuerySelection {
		return ReQuerySelection(jsonSerialization: [ReTerm.STATUS.rawValue, [self.jsonSerialization]])
	}

	public func sync() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.SYNC.rawValue, [self.jsonSerialization]])
	}

	public func wait() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.WAIT.rawValue, [self.jsonSerialization]])
	}

	public func get(primaryKey: AnyObject) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.GET.rawValue, [self.jsonSerialization, primaryKey]])
	}

	public func rebalance() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.REBALANCE.rawValue, [self.jsonSerialization]])
	}

	public func between(lower: ReQueryValue, _ upper: ReQueryValue) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.BETWEEN.rawValue, [self.jsonSerialization, lower.jsonSerialization, upper.jsonSerialization]])
	}

	public override func distinct() -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.DISTINCT.rawValue, [self.jsonSerialization]])
	}
}

public protocol ReQuery {
	var jsonSerialization: AnyObject { get }
}

public protocol ReQueryValue: ReQuery {
}

public enum ReTypeName: String {
	case Number = "number"
	case String = "string"
	case Array = "array"
	case Object = "object"
	case Binary = "binary"
}

public extension ReQuery {
	typealias Callback = (ReResponse) -> ()

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
		return ReDatum(jsonSerialization: [ReTerm.TYPE_OF.rawValue, [self.jsonSerialization]])
	}

	public func coerceTo(type: ReTypeName) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.COERCE_TO.rawValue, [self.jsonSerialization, type.rawValue]])
	}

	public func coerceTo(type: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.COERCE_TO.rawValue, [self.jsonSerialization, type.jsonSerialization]])
	}

	public func info() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INFO.rawValue, [self.jsonSerialization]])
	}
}

extension String: ReQueryValue {
	public var jsonSerialization: AnyObject { get { return self } }
}

extension Int: ReQueryValue {
	public var jsonSerialization: AnyObject { get { return self } }
}

extension Double: ReQueryValue {
	public var jsonSerialization: AnyObject { get { return self } }
}

extension Bool: ReQueryValue {
	public var jsonSerialization: AnyObject { get { return self } }
}

public extension ReQueryValue {
	public func add(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ADD.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func sub(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.SUB.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func mul(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MUL.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func div(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DIV.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func mod(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MOD.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func and(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.AND.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func or(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.OR.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func eq(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.EQ.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func ne(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.NE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func gt(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.GT.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func ge(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.GE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func lt(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.LT.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func le(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.LE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func not() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.NOT.rawValue, [self.jsonSerialization]])
	}

	public func round() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ROUND.rawValue, [self.jsonSerialization]])
	}

	public func ceil() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.CEIL.rawValue, [self.jsonSerialization]])
	}

	public func floor() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.FLOOR.rawValue, [self.jsonSerialization]])
	}

	public func toEpochTime() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TO_EPOCH_TIME.rawValue, [self.jsonSerialization]])
	}

	public func defaults(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DEFAULT.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func toJSON() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TO_JSON_STRING.rawValue, [self.jsonSerialization]])
	}

	public func toJsonString() -> ReQueryValue {
		return self.toJSON()
	}

	public subscript(key: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.BRACKET.rawValue, [self.jsonSerialization, key]])
	}
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
	public let jsonSerialization: AnyObject
	private static var parameterCounter = 0

	init(block: RePredicate) {
		let parameter = ReDatum(jsonSerialization: ++ReQueryLambda.parameterCounter)
		let parameterAccess = ReDatum(jsonSerialization: [ReTerm.VAR.rawValue, [parameter.jsonSerialization]])

		self.jsonSerialization = [
				ReTerm.FUNC.rawValue, [
						[ReTerm.MAKE_ARRAY.rawValue, [parameter.jsonSerialization]],
						block(parameterAccess).jsonSerialization
				]
		]
	}
}

public class ReQueryPoint: ReDatum {
	init(longitude: Double, latitude: Double) {
		super.init(jsonSerialization: [ReTerm.POINT.rawValue, [longitude, latitude]])
	}
}
