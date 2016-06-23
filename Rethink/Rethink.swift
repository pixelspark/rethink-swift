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
	private static func optargs<T: ReArg>(_ args: [T]) -> AnyObject {
		var dict: [String: AnyObject] = [:]
		for arg in args {
			assert(dict[arg.serialization.0] == nil, "an optional argument may only be specified once")
			dict[arg.serialization.0] = arg.serialization.1
		}
		return dict
	}

	public static func connect(_ url: URL, user: String = ReProtocol.defaultUser, password: String = ReProtocol.defaultPassword, version: ReProtocolVersion = .v1_0, callback: (ReError?, ReConnection) -> ()) {
		let c = ReConnection(url: url, protocolVersion: version)
		c.connect(user, password: password) { err in
			callback(err, c)
		}
	}

	public static func uuid() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.uuid.rawValue])
	}

	public static func db(_ name: String) -> ReQueryDatabase {
		return ReQueryDatabase(name: name)
	}

	public static func dbCreate(_ name: String) -> ReQuery {
		return ReDatum(jsonSerialization: [ReTerm.db_CREATE.rawValue, [name]])
	}

	public static func dbDrop(_ name: String) -> ReQuery {
		return ReDatum(jsonSerialization: [ReTerm.db_DROP.rawValue, [name]])
	}

	public static func dbList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.db_LIST.rawValue])
	}

	public static func point(_ longitude: Double, latitude: Double) -> ReQueryPoint {
		return ReQueryPoint(longitude: longitude, latitude:latitude)
	}

	public static func expr(_ string: String) -> ReQueryValue {
		return ReDatum(string: string)
	}

	public static func expr(_ double: Double) -> ReQueryValue {
		return ReDatum(double: double)
	}

	public static func expr(_ int: Int) -> ReQueryValue {
		return ReDatum(int: int)
	}

	public static func expr(_ binary: Data) -> ReQueryValue {
		return ReDatum(data: binary)
	}

	public static func expr(_ date: Date) -> ReQueryValue {
		return ReDatum(date: date)
	}

	public static func expr(_ bool: Bool) -> ReQueryValue {
		return ReDatum(bool: bool)
	}

	public static func expr() -> ReQueryValue {
		return ReDatum()
	}

	public static func expr(_ document: ReDocument) -> ReQueryValue {
		return ReDatum(document: document)
	}

	public static func expr(_ object: [String: ReQueryValue]) -> ReQueryValue {
		return ReDatum(object: object)
	}

	public static func expr(_ array: [ReQueryValue]) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.make_ARRAY.rawValue, array.map { return $0.jsonSerialization }])
	}

	public static func not(_ value: ReQueryValue) -> ReQueryValue {
		return value.not()
	}

	public static func random(_ lower: Int, _ upperOpen: Int) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.random.rawValue, [lower, upperOpen]])
	}

	public static func random(_ lower: Double, _ upperOpen: Double) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.random.rawValue, [lower, upperOpen], ["float": true]])
	}

	public static func random(_ lower: ReQueryValue, _ upperOpen: ReQueryValue, float: Bool = false) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.random.rawValue, [lower.jsonSerialization, upperOpen.jsonSerialization], ["float": float]])
	}

	public static func random() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.random.rawValue, []])
	}

	public static func round(_ value: ReQueryValue) -> ReQueryValue {
		return value.round()
	}

	public static func ceil(_ value: ReQueryValue) -> ReQueryValue {
		return value.ceil()
	}

	public static func floor(_ value: ReQueryValue) -> ReQueryValue {
		return value.floor()
	}

	public static func now() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.now.rawValue])
	}

	public static func ISO8601(_ date: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.iso8601.rawValue, [date]])
	}

	public static func error(_ message: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.error.rawValue, [message]])
	}

	public static func branch(_ test: ReQuery, ifTrue: ReQuery, ifFalse: ReQuery) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.branch.rawValue, [test.jsonSerialization, ifTrue.jsonSerialization, ifFalse.jsonSerialization]])
	}

	public static func object(_ key: ReQueryValue, value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.object.rawValue, [key.jsonSerialization, value.jsonSerialization]])
	}

	public static func range(_ start: ReQueryValue, _ end: ReQueryValue) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.range.rawValue, [start.jsonSerialization, end.jsonSerialization]])
	}

	public static func js(_ source: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.javascript.rawValue, [source]])
	}

	public static func json(_ source: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.json.rawValue, [source]])
	}

	public static func grant(_ userName: String, permissions: RePermission...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.grant.rawValue, [userName], R.optargs(permissions)])
	}

	public static var minVal: ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.minval.rawValue])
	}

	public static var maxVal: ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.maxval.rawValue])
	}

	/** Construct a circular line or polygon. A circle in RethinkDB is a polygon or line approximating a circle of a given
	 radius around a given center, consisting of a specified number of vertices (default 32).

	The center may be specified either by two floating point numbers, the latitude (−90 to 90) and longitude (−180 to 180) 
	of the point on a perfect sphere (see Geospatial support for more information on ReQL’s coordinate system), or by a 
	point object. The radius is a floating point number whose units are meters by default, although that may be changed 
	with the unit argument. */
	public static func circle(_ longitude: ReQueryValue, latitude: ReQueryValue, radius: ReQueryValue, options: ReCircleArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.circle.rawValue, [longitude.jsonSerialization, latitude.jsonSerialization, radius.jsonSerialization], R.optargs(options)])
	}

	public static func circle(_ point: ReQueryPoint, radius: ReQueryValue, options: ReCircleArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.circle.rawValue, [point.jsonSerialization, radius.jsonSerialization], R.optargs(options)])
	}

	/** Compute the distance between a point and another geometry object. At least one of the geometry objects specified
	must be a point. 
	
	If one of the objects is a polygon or a line, the point will be projected onto the line or polygon assuming a perfect 
	sphere model before the distance is computed (using the model specified with geoSystem). As a consequence, if the 
	polygon or line is extremely large compared to Earth’s radius and the distance is being computed with the default WGS84 
	model, the results of distance should be considered approximate due to the deviation between the ellipsoid and spherical 
	models. */
	public func distance(_ from: ReQueryGeometry, to: ReQueryGeometry, options: ReDistanceArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.distance.rawValue, [from.jsonSerialization, to.jsonSerialization], R.optargs(options)])
	}

	/** Convert a GeoJSON object to a ReQL geometry object.

	RethinkDB only allows conversion of GeoJSON objects which have ReQL equivalents: Point, LineString, and Polygon. 
	MultiPoint, MultiLineString, and MultiPolygon are not supported. (You could, however, store multiple points, lines and 
	polygons in an array and use a geospatial multi index with them.)

	Only longitude/latitude coordinates are supported. GeoJSON objects that use Cartesian coordinates, specify an altitude, 
	or specify their own coordinate reference system will be rejected. */
	public func geoJSON(_ json: ReQueryValue) -> ReQueryGeometry {
		return ReQueryGeometry(jsonSerialization: [ReTerm.geojson.rawValue, [json.jsonSerialization]])
	}

	/** Tests whether two geometry objects intersect with one another.  */
	public func intersects(_ geometry: ReQueryGeometry, with: ReQueryGeometry) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.intersects.rawValue, [geometry.jsonSerialization, with.jsonSerialization]])
	}

	public func line(_ from: ReQueryPoint, to: ReQueryPoint) -> ReQueryLine {
		return ReQueryLine(jsonSerialization: [ReTerm.line.rawValue, [from.jsonSerialization, to.jsonSerialization]])
	}
}

public class ReQueryDatabase: ReQuery {
	public let jsonSerialization: AnyObject

	private init(name: String) {
		self.jsonSerialization = [ReTerm.db.rawValue, [name]]
	}

	public func table(_ name: String, options: ReTableArg...) -> ReQueryTable {
		return ReQueryTable(database: self, name: name, options: options)
	}

	public func tableCreate(_ name: String, options: ReTableCreateArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.table_CREATE.rawValue, [self.jsonSerialization, name], R.optargs(options)])
	}

	public func tableDrop(_ name: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.table_DROP.rawValue, [self.jsonSerialization, name]])
	}

	public func tableList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.table_LIST.rawValue, [self.jsonSerialization]])
	}

	public func wait() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.wait.rawValue, [self.jsonSerialization]])
	}

	public func rebalance() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.rebalance.rawValue, [self.jsonSerialization]])
	}

	public func grant(_ userName: String, permissions: RePermission...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.grant.rawValue, [self.jsonSerialization, userName], R.optargs(permissions)])
	}
}

public class ReQuerySequence: ReQuery {
	public let jsonSerialization: AnyObject

	private init(jsonSerialization: AnyObject) {
		self.jsonSerialization = jsonSerialization
	}

	public func distinct() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.distinct.rawValue, [self.jsonSerialization]])
	}

	public func count() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.count.rawValue, [self.jsonSerialization]])
	}

	public func limit(_ count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.limit.rawValue, [self.jsonSerialization, count]])
	}

	public func skip(_ count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.skip.rawValue, [self.jsonSerialization, count]])
	}

	public func sample(_ count: Int) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.sample.rawValue, [self.jsonSerialization, count]])
	}

	public func nth(_ index: Int) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.nth.rawValue, [self.jsonSerialization, index]])
	}

	public func isEmpty() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.is_EMPTY.rawValue, [self.jsonSerialization]])
	}

	public func filter(_ specification: [String: ReQueryValue], options: ReFilterArg...) -> ReQuerySequence {
		var serialized: [String: AnyObject] = [:]
		for (k, v) in specification {
			serialized[k] = v.jsonSerialization
		}
		return ReQuerySequence(jsonSerialization: [ReTerm.filter.rawValue, [self.jsonSerialization, serialized], R.optargs(options)])
	}

	public func filter(_ predicate: RePredicate) -> ReQuerySequence {
		let fun = ReQueryLambda(block: predicate)
		return ReQuerySequence(jsonSerialization: [ReTerm.filter.rawValue, [self.jsonSerialization, fun.jsonSerialization]])
	}

	public func forEach(_ block: RePredicate) -> ReQuerySequence {
		let fun = ReQueryLambda(block: block)
		return ReQuerySequence(jsonSerialization: [ReTerm.for_EACH.rawValue, [self.jsonSerialization, fun.jsonSerialization]])
	}

	public func innerJoin(_ foreign: ReQuerySequence, predicate: RePredicate) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.inner_JOIN.rawValue, [self.jsonSerialization, foreign.jsonSerialization, ReQueryLambda(block: predicate).jsonSerialization]])
	}

	public func outerJoin(_ foreign: ReQuerySequence, predicate: RePredicate) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.outer_JOIN.rawValue, [self.jsonSerialization, foreign.jsonSerialization, ReQueryLambda(block: predicate).jsonSerialization]])
	}

	public func eqJoin(_ leftField: ReQueryValue, foreign: ReQueryTable, options: ReEqJoinArg...) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.eq_JOIN.rawValue, [self.jsonSerialization, leftField.jsonSerialization, foreign.jsonSerialization], R.optargs(options)])
	}

	public func map(_ mapper: ReQueryLambda) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.map.rawValue, [self.jsonSerialization, mapper.jsonSerialization]])
	}

	public func map(_ block: RePredicate) -> ReQuerySequence {
		return self.map(ReQueryLambda(block: block))
	}

	public func withFields(_ fields: [ReQueryValue]) -> ReQuerySequence {
		let values = fields.map({ e in return e.jsonSerialization })
		return ReQuerySequence(jsonSerialization: [ReTerm.with_FIELDS.rawValue, [self.jsonSerialization, [ReTerm.make_ARRAY.rawValue, values]]])
	}

	public func union(_ sequence: ReQuerySequence) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.union.rawValue, [self.jsonSerialization, sequence.jsonSerialization]])
	}

	public func delete(_ options: ReDeleteArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.delete.rawValue, [self.jsonSerialization], R.optargs(options)])
	}

	public func changes(_ options: ReChangesArg...) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.changes.rawValue, [self.jsonSerialization], R.optargs(options)])
	}

	/** In its first form, fold operates like reduce, returning a value by applying a combining function to each element 
	in a sequence, passing the current element and the previous reduction result to the function. However, fold has the 
	following differences from reduce:
	- it is guaranteed to proceed through the sequence from first element to last.
	- it passes an initial base value to the function with the first element in place of the previous reduction result. */
	public func fold(_ base: ReQueryValue, accumulator: ReQueryLambda, options: ReFoldArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.fold.rawValue, [self.jsonSerialization, base.jsonSerialization, accumulator.jsonSerialization], R.optargs(options)])
	}
}

public class ReQuerySelection: ReQuerySequence {
}

public class ReQueryStream: ReQuerySequence {
	public func zip() -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.zip.rawValue, [self.jsonSerialization]])
	}

	public override func sample(_ count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.sample.rawValue, [self.jsonSerialization, count]])
	}
}

public class ReQueryTable: ReQuerySequence {
	private init(database: ReQueryDatabase, name: String, options: [ReTableArg]) {
		let x = R.optargs(options)
		super.init(jsonSerialization: [ReTerm.table.rawValue, [database.jsonSerialization, name], x])
	}

	/** Insert documents into a table. */
	public func insert(_ documents: [ReDocument], options: ReInsertArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.insert.rawValue, [self.jsonSerialization, [ReTerm.make_ARRAY.rawValue, documents]], R.optargs(options)])
	}

	/** Insert documents into a table. */
	public func insert(_ objects: [ReQueryValue], options: ReInsertArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.insert.rawValue, [self.jsonSerialization, [ReTerm.make_ARRAY.rawValue, objects.map { return $0.jsonSerialization }]], R.optargs(options)])
	}

	/** Update JSON documents in a table. Accepts a JSON document, a ReQL expression, or a combination of the two. */
	public func update(_ changes: ReDocument, options: ReUpdateArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.update.rawValue, [self.jsonSerialization, changes], R.optargs(options)])
	}

	/** Replace documents in a table. Accepts a JSON document or a ReQL expression, and replaces the original document with
	 the new one. The new document must have the same primary key as the original document.

	The replace command can be used to both insert and delete documents. If the “replaced” document has a primary key that 
	doesn’t exist in the table, the document will be inserted; if an existing document is replaced with null, the document 
	will be deleted. Since update and replace operations are performed atomically, this allows atomic inserts and deletes 
	as well. */
	public func replace(_ changes: ReDocument, options: ReUpdateArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.replace.rawValue, [self.jsonSerialization, changes], R.optargs(options)])
	}

	/** Create a new secondary index on a table. Secondary indexes improve the speed of many read queries at the slight 
	cost of increased storage space and decreased write performance. 
	
	The indexFunction can be an anonymous function or a binary representation obtained from the function field of 
	indexStatus. If successful, createIndex will return an object of the form {"created": 1}. */
	public func indexCreate(_ indexName: String, indexFunction: ReQueryLambda? = nil, options: ReIndexCreateArg...) -> ReQueryValue {
		if let fn = indexFunction {
			 return ReDatum(jsonSerialization: [ReTerm.index_CREATE.rawValue, [self.jsonSerialization, indexName, fn.jsonSerialization], R.optargs(options)])
		}
		return ReDatum(jsonSerialization: [ReTerm.index_CREATE.rawValue, [self.jsonSerialization, indexName], R.optargs(options)])
	}

	public func indexWait() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.index_WAIT.rawValue, [self.jsonSerialization]])
	}

	public func indexDrop(_ name: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.index_DROP.rawValue, [self.jsonSerialization, name]])
	}

	public func indexList() -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.index_LIST.rawValue, [self.jsonSerialization]])
	}

	/** Rename an existing secondary index on a table.  */
	public func indexRename(_ renameIndex: String, to: String, options: ReIndexRenameArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.index_RENAME.rawValue, [self.jsonSerialization, renameIndex, to], R.optargs(options)])
	}

	/** Get the status of the specified indexes on this table, or the status of all indexes on this table if no indexes 
	are specified. */
	public func indexStatus(_ indices: String...) -> ReQueryValue {
		var params: [AnyObject] = [self.jsonSerialization]
		indices.forEach { params.append($0) }
		return ReDatum(jsonSerialization: [ReTerm.index_STATUS.rawValue, params])
	}

	public func status() -> ReQuerySelection {
		return ReQuerySelection(jsonSerialization: [ReTerm.status.rawValue, [self.jsonSerialization]])
	}

	public func sync() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.sync.rawValue, [self.jsonSerialization]])
	}

	public func wait() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.wait.rawValue, [self.jsonSerialization]])
	}

	public func get(_ primaryKey: AnyObject) -> ReQueryRow {
		return ReQueryRow(jsonSerialization: [ReTerm.get.rawValue, [self.jsonSerialization, primaryKey]])
	}

	public func getAll(_ key: ReQueryValue, index: String) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.get_ALL.rawValue, [self.jsonSerialization, key.jsonSerialization], ["index": index]])
	}

	public func rebalance() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.rebalance.rawValue, [self.jsonSerialization]])
	}

	public func between(_ lower: ReQueryValue, _ upper: ReQueryValue) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.between.rawValue, [self.jsonSerialization, lower.jsonSerialization, upper.jsonSerialization]])
	}

	public override func distinct() -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.distinct.rawValue, [self.jsonSerialization]])
	}

	public func grant(_ userName: String, permissions: RePermission...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.grant.rawValue, [self.jsonSerialization, userName], R.optargs(permissions)])
	}

	public func getIntersecting(_ geometry: ReQueryGeometry, options: ReIntersectingArg...) -> ReQuerySelection {
		return ReQuerySelection(jsonSerialization: [ReTerm.get_INTERSECTING.rawValue, [self.jsonSerialization, geometry.jsonSerialization], R.optargs(options)])
	}
}

public protocol ReQuery {
	var jsonSerialization: AnyObject { get }
}

public protocol ReQueryValue: ReQuery {
}

public class ReQueryRow: ReDatum {
	public func update(_ changes: ReDocument) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.update.rawValue, [self.jsonSerialization, changes]])
	}

	public func delete(_ options: ReDeleteArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.delete.rawValue, [self.jsonSerialization], R.optargs(options)])
	}

	public func keys() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.keys.rawValue, [self.jsonSerialization]])
	}
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

	public func run(_ connection: ReConnection, callback: Callback) {
		let query: [AnyObject] = [ReProtocol.ReQueryType.start.rawValue, self.jsonSerialization];

		do {
			let json = try JSONSerialization.data(withJSONObject: query, options: [])
			//let ss = NSString(data: json, encoding: NSUTF8StringEncoding)!
			//print("JSON=\(ss)")
			try connection.startQuery(json, callback: callback)
		}
		catch {
			callback(ReResponse.error("An unknown error occurred"))
		}
	}

	public func typeOf() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.type_OF.rawValue, [self.jsonSerialization]])
	}

	public func coerceTo(_ type: ReTypeName) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.coerce_TO.rawValue, [self.jsonSerialization, type.rawValue]])
	}

	public func coerceTo(_ type: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.coerce_TO.rawValue, [self.jsonSerialization, type.jsonSerialization]])
	}

	public func info() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.info.rawValue, [self.jsonSerialization]])
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
	public func add(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.add.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func sub(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.sub.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func mul(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.mul.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func div(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.div.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func mod(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.mod.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func and(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.and.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func or(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.or.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func eq(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.eq.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func ne(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ne.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func gt(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.gt.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func ge(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ge.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func lt(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.lt.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func le(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.le.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func not() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.not.rawValue, [self.jsonSerialization]])
	}

	public func round() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.round.rawValue, [self.jsonSerialization]])
	}

	public func ceil() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.ceil.rawValue, [self.jsonSerialization]])
	}

	public func floor() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.floor.rawValue, [self.jsonSerialization]])
	}

	public func toEpochTime() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.to_EPOCH_TIME.rawValue, [self.jsonSerialization]])
	}

	public func defaults(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.default.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func match(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.match.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func toJSON() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.to_JSON_STRING.rawValue, [self.jsonSerialization]])
	}

	public func toJsonString() -> ReQueryValue {
		return self.toJSON()
	}

	public func upcase() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.upcase.rawValue, [self.jsonSerialization]])
	}

	public func downcase() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.downcase.rawValue, [self.jsonSerialization]])
	}

	public func xor(_ other: ReQueryValue) -> ReQueryValue {
		return self.and(other.not()).or(self.not().and(other))
	}

	public func merge(_ value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.merge.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func branch(_ ifTrue: ReQueryValue, _ ifFalse: ReQueryValue) -> ReQueryValue {
		return R.branch(self, ifTrue: ifTrue, ifFalse: ifFalse)
	}

	public subscript(key: String) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.bracket.rawValue, [self.jsonSerialization, key]])
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
		ReQueryLambda.parameterCounter += 1
		let p = ReQueryLambda.parameterCounter
		let parameter = ReDatum(jsonSerialization: p)
		let parameterAccess = ReDatum(jsonSerialization: [ReTerm.var.rawValue, [parameter.jsonSerialization]])

		self.jsonSerialization = [
				ReTerm.func.rawValue, [
						[ReTerm.make_ARRAY.rawValue, [parameter.jsonSerialization]],
						block(parameterAccess).jsonSerialization
				]
		]
	}
}

public class ReQueryGeometry: ReDatum {
	/** Compute the distance between a point and another geometry object. At least one of the geometry objects specified 
	must be a point.

	If one of the objects is a polygon or a line, the point will be projected onto the line or polygon assuming a perfect
	sphere model before the distance is computed (using the model specified with geoSystem). As a consequence, if the
	polygon or line is extremely large compared to Earth’s radius and the distance is being computed with the default WGS84
	model, the results of distance should be considered approximate due to the deviation between the ellipsoid and spherical
	models. */
	public func distance(_ geometry: ReQueryGeometry, options: ReDistanceArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.distance.rawValue, [self.jsonSerialization, geometry.jsonSerialization], R.optargs(options)])
	}

	/** Convert a ReQL geometry object to a GeoJSON object. */
	public func toGeoJSON() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.to_GEOJSON.rawValue, [self.jsonSerialization]])
	}

	/** Tests whether two geometry objects intersect with one another.  */
	public func intersects(_ geometry: ReQueryGeometry) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.intersects.rawValue, [self.jsonSerialization, geometry.jsonSerialization]])
	}
}

public class ReQueryPolygon: ReQueryGeometry {

}

public class ReQueryLine: ReQueryGeometry {
	/** Convert a Line object into a Polygon object. If the last point does not specify the same coordinates as the first 
	point, polygon will close the polygon by connecting them.

	Longitude (−180 to 180) and latitude (−90 to 90) of vertices are plotted on a perfect sphere. See Geospatial support 
	for more information on ReQL’s coordinate system.

	If the last point does not specify the same coordinates as the first point, polygon will close the polygon by 
	connecting them. You cannot directly construct a polygon with holes in it using polygon, but you can use polygonSub to
	use a second polygon within the interior of the first to define a hole. */
	public func fill() -> ReQueryPolygon {
		return ReQueryPolygon(jsonSerialization: [ReTerm.fill.rawValue, [self.jsonSerialization]])
	}
}

public class ReQueryPoint: ReQueryGeometry {
	init(longitude: Double, latitude: Double) {
		super.init(jsonSerialization: [ReTerm.point.rawValue, [longitude, latitude]])
	}
}
