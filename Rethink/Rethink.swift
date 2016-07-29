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
	private static func optargs<T: ReArg>(args: [T]) -> AnyObject {
		var dict: [String: AnyObject] = [:]
		for arg in args {
			assert(dict[arg.serialization.0] == nil, "an optional argument may only be specified once")
			dict[arg.serialization.0] = arg.serialization.1
		}
		return dict
	}

	public static func connect(url: NSURL, user: String = ReProtocol.defaultUser, password: String = ReProtocol.defaultPassword, version: ReProtocolVersion = .V1_0, callback: (ReError?, ReConnection) -> ()) {
		let c = ReConnection(url: url, protocolVersion: version)
		c.connect(user, password: password) { err in
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

	public static func random(lower: ReQueryValue, _ upperOpen: ReQueryValue, float: Bool = false) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.RANDOM.rawValue, [lower.jsonSerialization, upperOpen.jsonSerialization], ["float": float]])
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

	public static func grant(userName: String, permissions: RePermission...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.GRANT.rawValue, [userName], R.optargs(permissions)])
	}

	public static var minVal: ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MINVAL.rawValue])
	}

	public static var maxVal: ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MAXVAL.rawValue])
	}

	/** Construct a circular line or polygon. A circle in RethinkDB is a polygon or line approximating a circle of a given
	 radius around a given center, consisting of a specified number of vertices (default 32).

	The center may be specified either by two floating point numbers, the latitude (−90 to 90) and longitude (−180 to 180) 
	of the point on a perfect sphere (see Geospatial support for more information on ReQL’s coordinate system), or by a 
	point object. The radius is a floating point number whose units are meters by default, although that may be changed 
	with the unit argument. */
	public static func circle(longitude: ReQueryValue, latitude: ReQueryValue, radius: ReQueryValue, options: ReCircleArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.CIRCLE.rawValue, [longitude.jsonSerialization, latitude.jsonSerialization, radius.jsonSerialization], R.optargs(options)])
	}

	public static func circle(point: ReQueryPoint, radius: ReQueryValue, options: ReCircleArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.CIRCLE.rawValue, [point.jsonSerialization, radius.jsonSerialization], R.optargs(options)])
	}

	/** Compute the distance between a point and another geometry object. At least one of the geometry objects specified
	must be a point. 
	
	If one of the objects is a polygon or a line, the point will be projected onto the line or polygon assuming a perfect 
	sphere model before the distance is computed (using the model specified with geoSystem). As a consequence, if the 
	polygon or line is extremely large compared to Earth’s radius and the distance is being computed with the default WGS84 
	model, the results of distance should be considered approximate due to the deviation between the ellipsoid and spherical 
	models. */
	public func distance(from: ReQueryGeometry, to: ReQueryGeometry, options: ReDistanceArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DISTANCE.rawValue, [from.jsonSerialization, to.jsonSerialization], R.optargs(options)])
	}

	/** Convert a GeoJSON object to a ReQL geometry object.

	RethinkDB only allows conversion of GeoJSON objects which have ReQL equivalents: Point, LineString, and Polygon. 
	MultiPoint, MultiLineString, and MultiPolygon are not supported. (You could, however, store multiple points, lines and 
	polygons in an array and use a geospatial multi index with them.)

	Only longitude/latitude coordinates are supported. GeoJSON objects that use Cartesian coordinates, specify an altitude, 
	or specify their own coordinate reference system will be rejected. */
	public func geoJSON(json: ReQueryValue) -> ReQueryGeometry {
		return ReQueryGeometry(jsonSerialization: [ReTerm.GEOJSON.rawValue, [json.jsonSerialization]])
	}

	/** Tests whether two geometry objects intersect with one another.  */
	public func intersects(geometry: ReQueryGeometry, with: ReQueryGeometry) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INTERSECTS.rawValue, [geometry.jsonSerialization, with.jsonSerialization]])
	}

	public func line(from: ReQueryPoint, to: ReQueryPoint) -> ReQueryLine {
		return ReQueryLine(jsonSerialization: [ReTerm.LINE.rawValue, [from.jsonSerialization, to.jsonSerialization]])
	}
    
    public static func asc(key: String) -> ReQueryValue {
        return ReDatum(jsonSerialization: [ReTerm.ASC.rawValue, [key.jsonSerialization]])
    }

    public static func desc(key: String) -> ReQueryValue {
        return ReDatum(jsonSerialization: [ReTerm.DESC.rawValue, [key.jsonSerialization]])
    }
}

public class ReQueryDatabase: ReQuery {
	public let jsonSerialization: AnyObject

	private init(name: String) {
		self.jsonSerialization = [ReTerm.DB.rawValue, [name]]
	}

	public func table(name: String, options: ReTableArg...) -> ReQueryTable {
		return ReQueryTable(database: self, name: name, options: options)
	}

	public func tableCreate(name: String, options: ReTableCreateArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TABLE_CREATE.rawValue, [self.jsonSerialization, name], R.optargs(options)])
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

	public func grant(userName: String, permissions: RePermission...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.GRANT.rawValue, [self.jsonSerialization, userName], R.optargs(permissions)])
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

	public func filter(specification: [String: ReQueryValue], options: ReFilterArg...) -> ReQuerySequence {
		var serialized: [String: AnyObject] = [:]
		for (k, v) in specification {
			serialized[k] = v.jsonSerialization
		}
		return ReQuerySequence(jsonSerialization: [ReTerm.FILTER.rawValue, [self.jsonSerialization, serialized], R.optargs(options)])
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

	public func eqJoin(leftField: ReQueryValue, foreign: ReQueryTable, options: ReEqJoinArg...) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.EQ_JOIN.rawValue, [self.jsonSerialization, leftField.jsonSerialization, foreign.jsonSerialization], R.optargs(options)])
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

	public func delete(options: ReDeleteArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DELETE.rawValue, [self.jsonSerialization], R.optargs(options)])
	}

	public func changes(options: ReChangesArg...) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.CHANGES.rawValue, [self.jsonSerialization], R.optargs(options)])
	}

	/** In its first form, fold operates like reduce, returning a value by applying a combining function to each element 
	in a sequence, passing the current element and the previous reduction result to the function. However, fold has the 
	following differences from reduce:
	- it is guaranteed to proceed through the sequence from first element to last.
	- it passes an initial base value to the function with the first element in place of the previous reduction result. */
	public func fold(base: ReQueryValue, accumulator: ReQueryLambda, options: ReFoldArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.FOLD.rawValue, [self.jsonSerialization, base.jsonSerialization, accumulator.jsonSerialization], R.optargs(options)])
	}
    
    public func withoutFields(fields: [ReQueryValue]) -> ReQuerySequence {
        let values = fields.map({ e in return e.jsonSerialization })
        return ReQuerySequence(jsonSerialization: [ReTerm.WITHOUT.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, values]]])
    }
    
    public func orderBy(sortKey sortKey: ReQueryValue) -> ReQuerySequence {
        var querySequence: [AnyObject] = [
            ReTerm.ORDER_BY.rawValue,
            [self.jsonSerialization, sortKey.jsonSerialization],
        ]
        
        return ReQuerySequence(
            jsonSerialization: querySequence
        )
    }

    public func hasFields(field: ReQueryValue) -> ReQuerySequence {
        return self.hasFields([field])
    }

    public func hasFields(fields: [ReQueryValue]) -> ReQuerySequence {
        let values = fields.map({ e in return e.jsonSerialization})
        return ReQuerySequence(jsonSerialization: [ReTerm.HAS_FIELDS.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, values]]])
    }
}

public class ReQuerySelection: ReQuerySequence {
}

public class ReQueryStream: ReQuerySequence {
	public func zip() -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.ZIP.rawValue, [self.jsonSerialization]])
	}

	public override func sample(count: Int) -> ReQueryStream {
		return ReQueryStream(jsonSerialization: [ReTerm.SAMPLE.rawValue, [self.jsonSerialization, count]])
	}
}

public class ReQueryTable: ReQuerySequence {
	private init(database: ReQueryDatabase, name: String, options: [ReTableArg]) {
		let x = R.optargs(options)
		super.init(jsonSerialization: [ReTerm.TABLE.rawValue, [database.jsonSerialization, name], x])
	}

	/** Insert documents into a table. */
	public func insert(documents: [ReDocument], options: ReInsertArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INSERT.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, documents]], R.optargs(options)])
	}

	/** Insert documents into a table. */
	public func insert(objects: [ReQueryValue], options: ReInsertArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INSERT.rawValue, [self.jsonSerialization, [ReTerm.MAKE_ARRAY.rawValue, objects.map { return $0.jsonSerialization }]], R.optargs(options)])
	}

	/** Update JSON documents in a table. Accepts a JSON document, a ReQL expression, or a combination of the two. */
	public func update(changes: ReDocument, options: ReUpdateArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.UPDATE.rawValue, [self.jsonSerialization, changes], R.optargs(options)])
	}

	/** Replace documents in a table. Accepts a JSON document or a ReQL expression, and replaces the original document with
	 the new one. The new document must have the same primary key as the original document.

	The replace command can be used to both insert and delete documents. If the “replaced” document has a primary key that 
	doesn’t exist in the table, the document will be inserted; if an existing document is replaced with null, the document 
	will be deleted. Since update and replace operations are performed atomically, this allows atomic inserts and deletes 
	as well. */
	public func replace(changes: ReDocument, options: ReUpdateArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.REPLACE.rawValue, [self.jsonSerialization, changes], R.optargs(options)])
	}

	/** Create a new secondary index on a table. Secondary indexes improve the speed of many read queries at the slight 
	cost of increased storage space and decreased write performance. 
	
	The indexFunction can be an anonymous function or a binary representation obtained from the function field of 
	indexStatus. If successful, createIndex will return an object of the form {"created": 1}. */
	public func indexCreate(indexName: String, indexFunction: ReQueryLambda? = nil, options: ReIndexCreateArg...) -> ReQueryValue {
		if let fn = indexFunction {
			 return ReDatum(jsonSerialization: [ReTerm.INDEX_CREATE.rawValue, [self.jsonSerialization, indexName, fn.jsonSerialization], R.optargs(options)])
		}
		return ReDatum(jsonSerialization: [ReTerm.INDEX_CREATE.rawValue, [self.jsonSerialization, indexName], R.optargs(options)])
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

	/** Rename an existing secondary index on a table.  */
	public func indexRename(renameIndex: String, to: String, options: ReIndexRenameArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INDEX_RENAME.rawValue, [self.jsonSerialization, renameIndex, to], R.optargs(options)])
	}

	/** Get the status of the specified indexes on this table, or the status of all indexes on this table if no indexes 
	are specified. */
	public func indexStatus(indices: String...) -> ReQueryValue {
		var params: [AnyObject] = [self.jsonSerialization]
		indices.forEach { params.append($0) }
		return ReDatum(jsonSerialization: [ReTerm.INDEX_STATUS.rawValue, params])
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

	public func get(primaryKey: AnyObject) -> ReQueryRow {
		return ReQueryRow(jsonSerialization: [ReTerm.GET.rawValue, [self.jsonSerialization, primaryKey]])
	}

	public func getAll(key: ReQueryValue, index: String) -> ReQuerySequence {
		return ReQuerySequence(jsonSerialization: [ReTerm.GET_ALL.rawValue, [self.jsonSerialization, key.jsonSerialization], ["index": index]])
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

	public func grant(userName: String, permissions: RePermission...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.GRANT.rawValue, [self.jsonSerialization, userName], R.optargs(permissions)])
	}

	public func getIntersecting(geometry: ReQueryGeometry, options: ReIntersectingArg...) -> ReQuerySelection {
		return ReQuerySelection(jsonSerialization: [ReTerm.GET_INTERSECTING.rawValue, [self.jsonSerialization, geometry.jsonSerialization], R.optargs(options)])
	}
    
    public func getAll(key: [ReQueryValue], index: String) -> ReQuerySequence {
        return ReQuerySequence(
            jsonSerialization: [ReTerm.GET_ALL.rawValue,
                [self.jsonSerialization] + key.map({$0.jsonSerialization}),
                ["index": index]
            ]
        )
    }
    
    public func orderBy(index index: ReQueryValue) -> ReQuerySequence {
        var querySequence: [AnyObject] = [
            ReTerm.ORDER_BY.rawValue,
            [self.jsonSerialization]
        ]
        
        querySequence.append(["index": index.jsonSerialization])
        
        return ReQuerySequence(
            jsonSerialization: querySequence
        )
    }
}

public protocol ReQuery {
	var jsonSerialization: AnyObject { get }
}

public protocol ReQueryValue: ReQuery {
}

public class ReQueryRow: ReDatum {
	public func update(changes: ReDocument) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.UPDATE.rawValue, [self.jsonSerialization, changes]])
	}

	public func delete(options: ReDeleteArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DELETE.rawValue, [self.jsonSerialization], R.optargs(options)])
	}

	public func keys() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.KEYS.rawValue, [self.jsonSerialization]])
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

	public func match(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MATCH.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func toJSON() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TO_JSON_STRING.rawValue, [self.jsonSerialization]])
	}

	public func toJsonString() -> ReQueryValue {
		return self.toJSON()
	}

	public func upcase() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.UPCASE.rawValue, [self.jsonSerialization]])
	}

	public func downcase() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DOWNCASE.rawValue, [self.jsonSerialization]])
	}

	public func xor(other: ReQueryValue) -> ReQueryValue {
		return self.and(other.not()).or(self.not().and(other))
	}

	public func merge(value: ReQueryValue) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.MERGE.rawValue, [self.jsonSerialization, value.jsonSerialization]])
	}

	public func branch(ifTrue: ReQueryValue, _ ifFalse: ReQueryValue) -> ReQueryValue {
		return R.branch(self, ifTrue: ifTrue, ifFalse: ifFalse)
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
		ReQueryLambda.parameterCounter += 1
		let p = ReQueryLambda.parameterCounter
		let parameter = ReDatum(jsonSerialization: p)
		let parameterAccess = ReDatum(jsonSerialization: [ReTerm.VAR.rawValue, [parameter.jsonSerialization]])

		self.jsonSerialization = [
				ReTerm.FUNC.rawValue, [
						[ReTerm.MAKE_ARRAY.rawValue, [parameter.jsonSerialization]],
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
	public func distance(geometry: ReQueryGeometry, options: ReDistanceArg...) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.DISTANCE.rawValue, [self.jsonSerialization, geometry.jsonSerialization], R.optargs(options)])
	}

	/** Convert a ReQL geometry object to a GeoJSON object. */
	public func toGeoJSON() -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.TO_GEOJSON.rawValue, [self.jsonSerialization]])
	}

	/** Tests whether two geometry objects intersect with one another.  */
	public func intersects(geometry: ReQueryGeometry) -> ReQueryValue {
		return ReDatum(jsonSerialization: [ReTerm.INTERSECTS.rawValue, [self.jsonSerialization, geometry.jsonSerialization]])
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
		return ReQueryPolygon(jsonSerialization: [ReTerm.FILL.rawValue, [self.jsonSerialization]])
	}
}

public class ReQueryPoint: ReQueryGeometry {
	init(longitude: Double, latitude: Double) {
		super.init(jsonSerialization: [ReTerm.POINT.rawValue, [longitude, latitude]])
	}
}
