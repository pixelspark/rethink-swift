import Foundation

public enum ReTableReadMode: String {
	case Single = "single"
	case Majority = "majority"
	case Outdated = "outdated"
}

public enum ReTableIdentifierFormat: String {
	case Name = "name"
	case UUID = "uuid"
}

/** Optional arguments are instances of ReArg. */
public protocol ReArg {
	var serialization: (String, AnyObject) { get }
}

/** Optional arguments for the R.table command. */
public enum ReTableArg: ReArg {
	case ReadMode(ReTableReadMode)
	case IdentifierFormat(ReTableIdentifierFormat)

	public var serialization: (String, AnyObject) {
		switch self {
		case .ReadMode(let rm): return ("read_mode", rm.rawValue)
		case .IdentifierFormat(let i): return ("identifier_format", i.rawValue)
		}
	}
}

public enum ReFilterArg: ReArg {
	case Default(AnyObject)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Default(let a): return ("default", a)
		}
	}
}

public enum ReTableDurability: String {
	case Soft = "soft"
	case Hard = "hard"
}

public enum ReTableCreateArg: ReArg {
	case PrimaryKey(String)
	case Durability(ReTableDurability)
	case Shards(Int)
	case Replicas(Int)

	public var serialization: (String, AnyObject) {
		switch self {
		case .PrimaryKey(let p): return ("primary_key", p)
		case .Durability(let d): return ("durability", d.rawValue)
		case .Shards(let s): return ("shards", s)
		case .Replicas(let r): return ("replicas", r)
		}
	}
}

public enum ReIndexCreateArg: ReArg {
	case Multi(Bool)
	case Geo(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Multi(let m): return ("multi", m)
		case .Geo(let g): return ("geo", g)
		}
	}
}

public enum ReIndexRenameArg: ReArg {
	/** If the optional argument overwrite is specified as true, a previously existing index with the new name will be 
	deleted and the index will be renamed. If overwrite is false (the default) an error will be raised if the new index 
	name already exists. */
	case Overwrite(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Overwrite(let o): return ("overwrite", o)
		}
	}
}

public enum RePermission: ReArg {
	case Read(Bool)
	case Write(Bool)
	case Connect(Bool)
	case Config(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Read(let b): return ("read", b)
		case .Write(let b): return ("write", b)
		case .Connect(let b): return ("connect", b)
		case .Config(let b): return ("config", b)
		}
	}
}

public enum ReChangesArg: ReArg {
	/** squash: Controls how change notifications are batched. Acceptable values are true, false and a numeric value:
	- true: When multiple changes to the same document occur before a batch of notifications is sent, the changes are 
	  "squashed" into one change. The client receives a notification that will bring it fully up to date with the server.
	- false: All changes will be sent to the client verbatim. This is the default.
	- n: A numeric value (floating point). Similar to true, but the server will wait n seconds to respond in order to 
	  squash as many changes together as possible, reducing network traffic. The first batch will always be returned 
	  immediately. */
	case Squash(Bool, n: Double?)

	/** changefeedQueueSize: the number of changes the server will buffer between client reads before it starts dropping 
	changes and generates an error (default: 100,000). */
	case ChangeFeedQueueSize(Int)

	/** includeInitial: if true, the changefeed stream will begin with the current contents of the table or selection 
	being monitored. These initial results will have new_val fields, but no old_val fields. The initial results may be 
	intermixed with actual changes, as long as an initial result for the changed document has already been given. If an 
	initial result for a document has been sent and a change is made to that document that would move it to the unsent 
	part of the result set (e.g., a changefeed monitors the top 100 posters, the first 50 have been sent, and poster 48 
	has become poster 52), an “uninitial” notification will be sent, with an old_val field but no new_val field.*/
	case IncludeInitial(Bool)

	/** includeStates: if true, the changefeed stream will include special status documents consisting of the field state
	and a string indicating a change in the feed’s state. These documents can occur at any point in the feed between the 
	notification documents described below. If includeStates is false (the default), the status documents will not be sent.*/
	case IncludeStates(Bool)

	/** includeOffsets: if true, a changefeed stream on an orderBy.limit changefeed will include old_offset and new_offset 
	fields in status documents that include old_val and new_val. This allows applications to maintain ordered lists of the
	stream’s result set. If old_offset is set and not null, the element at old_offset is being deleted; if new_offset is 
	set and not null, then new_val is being inserted at new_offset. Setting includeOffsets to true on a changefeed that 
	does not support it will raise an error.*/
	case IncludeOffsets(Bool)

	/** includeTypes: if true, every result on a changefeed will include a type field with a string that indicates the 
	kind of change the result represents: add, remove, change, initial, uninitial, state. Defaults to false.*/
	case IncludeTypes(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Squash(let b, let i):
			assert(!(i != nil && !b), "Do not specify a time interval when squashing is to be disabled")
			if let interval = i where b {
				 return ("squash", interval)
			}
			else {
				return ("squash", b)
			}

		case .ChangeFeedQueueSize(let i): return ("changefeed_queue_size", i)
		case .IncludeInitial(let b): return ("include_initial", b)
		case .IncludeStates(let b): return ("include_states", b)
		case .IncludeOffsets(let b): return ("include_offsets", b)
		case .IncludeTypes(let b): return ("include_types", b)
		}
	}
}

public enum ReFoldArg: ReArg {
	/** When an emit function is provided, fold will:
	- proceed through the sequence in order and take an initial base value, as above.
	- for each element in the sequence, call both the combining function and a separate emitting function with the current 
	  element and previous reduction result.
	- optionally pass the result of the combining function to the emitting function.
	If provided, the emitting function must return a list. */
	case Emit(ReQueryLambda)
	case FinalEmit(ReQueryLambda)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Emit(let r): return ("emit", r)
		case .FinalEmit(let r): return ("final_emit", r)
		}
	}
}

public enum ReEqJoinArg: ReArg {
	/** The results from eqJoin are, by default, not ordered. The optional ordered: true parameter will cause eqJoin to 
	order the output based on the left side input stream. (If there are multiple matches on the right side for a document 
	on the left side, their order is not guaranteed even if ordered is true.) Requiring ordered results can significantly 
	slow down eqJoin, and in many circumstances this ordering will not be required. (See the first example, in which 
	ordered results are obtained by using orderBy after eqJoin.) */
	case Ordered(Bool)
	case Index(String)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Ordered(let o): return ("ordered", o)
		case .Index(let i): return ("index", i)
		}
	}
}

public enum ReDurability: String {
	case Hard = "hard"
	case Soft = "soft"
}

public enum ReConflictResolution: String {
	/** Do not insert the new document and record the conflict as an error. This is the default. */
	case Error = "error"

	/** Replace the old document in its entirety with the new one. */
	case Replace = "replace"

	/** Update fields of the old document with fields from the new one. */
	case Update = "update"
}

public enum ReInsertArg: ReArg {
	/** This option will override the table or query’s durability setting (set in run). In soft durability mode RethinkDB 
	will acknowledge the write immediately after receiving and caching it, but before the write has been committed to disk. */
	case Durability(ReDurability)

	/** true: return a changes array consisting of old_val/new_val objects describing the changes made, only including the 
	documents actually updated. false: do not return a changes array (the default). */
	case ReturnChanges(Bool)

	/** Determine handling of inserting documents with the same primary key as existing entries.  */
	case Conflict(ReConflictResolution)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Durability(let d): return ("durability", d.rawValue)
		case .ReturnChanges(let r): return ("return_changes", r)
		case .Conflict(let r): return ("conflict", r.rawValue)
		}
	}
}

public enum ReUpdateArg: ReArg {
	/** This option will override the table or query’s durability setting (set in run). In soft durability mode RethinkDB
	will acknowledge the write immediately after receiving and caching it, but before the write has been committed to disk. */
	case Durability(ReDurability)

	/** true: return a changes array consisting of old_val/new_val objects describing the changes made, only including the
	documents actually updated. false: do not return a changes array (the default). */
	case ReturnChanges(Bool)

	/** If set to true, executes the update and distributes the result to replicas in a non-atomic fashion. This flag is 
	required to perform non-deterministic updates, such as those that require reading data from another table. */
	case NonAtomic(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Durability(let d): return ("durability", d.rawValue)
		case .ReturnChanges(let r): return ("return_changes", r)
		case .NonAtomic(let b): return ("non_atomic", b)
		}
	}
}

public enum ReDeleteArg: ReArg {
	/** This option will override the table or query’s durability setting (set in run). In soft durability mode RethinkDB
	will acknowledge the write immediately after receiving and caching it, but before the write has been committed to disk. */
	case Durability(ReDurability)

	/** true: return a changes array consisting of old_val/new_val objects describing the changes made, only including the
	documents actually updated. false: do not return a changes array (the default). */
	case ReturnChanges(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Durability(let d): return ("durability", d.rawValue)
		case .ReturnChanges(let r): return ("return_changes", r)
		}
	}
}

public enum ReUnit: String {
	case Meter = "m"
	case Kilometer = "km"
	case InternationalMile = "mi"
	case NauticalMile = "nm"
	case InternationalFoot = "ft"
}

public enum ReGeoSystem: String {
	case WGS84 = "WGS84"
	case UnitSphere = "unit_sphere"
}

public enum ReCircleArg: ReArg {
	/** The number of vertices in the polygon or line. Defaults to 32. */
	case NumVertices(Int)

	/** The reference ellipsoid to use for geographic coordinates. Possible values are WGS84 (the default), a common 
	standard for Earth’s geometry, or unit_sphere, a perfect sphere of 1 meter radius. */
	case GeoSystem(ReGeoSystem)

	/** Unit for the radius distance. */
	case Unit(ReUnit)

	/** If true (the default) the circle is filled, creating a polygon; if false the circle is unfilled (creating a line). */
	case Fill(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .NumVertices(let n): return ("num_vertices", n)
		case .GeoSystem(let s): return ("geo_system", s.rawValue)
		case .Unit(let u): return ("unit", u.rawValue)
		case .Fill(let b): return ("fill", b)
		}
	}
}

public enum ReDistanceArg: ReArg {
	case GeoSystem(ReGeoSystem)
	case Unit(ReUnit)

	public var serialization: (String, AnyObject) {
		switch self {
		case .GeoSystem(let s): return ("geo_system", s.rawValue)
		case .Unit(let u): return ("unit", u.rawValue)
		}
	}
}

public enum ReIntersectingArg: ReArg {
	/** The index argument is mandatory. This command returns the same results as 
	table.filter(r.row('index').intersects(geometry)). The total number of results is limited to the array size limit which 
	defaults to 100,000, but can be changed with the arrayLimit option to run. */
	case Index(String)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Index(let s): return ("index", s)
		}
	}
}