using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;
using Azavea.Open.DAO.Criteria;
using Azavea.Open.DAO.Criteria.Spatial;
using Azavea.Open.DAO.SQL;
using GeoAPI.Geometries;
using NetTopologySuite.IO;

namespace Azavea.Open.DAO.Odp
{
    public class OdpSdeStDaLayer : SqlDaDdlLayer
    {
        private readonly WKTWriter _wktWriter = new SdeStWktWriter();
        private readonly WKTReader _wktReader = new WKTReader();

        /// <summary>
        /// Construct the layer.  Should typically be called only by the appropriate
        /// ConnectionDescriptor.
        /// </summary>
        /// <param name="connDesc">Connection to the Oracle DB we'll be using.</param>
        public OdpSdeStDaLayer(OdpDescriptor connDesc)
            : base(connDesc, true)
        {
            _coerceableTypes = new Dictionary<Type, TypeCoercionDelegate>();
            _coerceableTypes.Add(typeof(IGeometry), CreateGeometry);
        }

        /// <exclude/>
        public override IDaQuery CreateQuery(ClassMapping mapping, DaoCriteria crit)
        {
            // Overridden to handle IGeometry types, and to always fully qualify column names.
            SqlDaQuery retVal = _sqlQueryCache.Get();
            retVal.Sql.Append("SELECT ");
            foreach (string attName in mapping.AllDataColsByObjAttrs.Keys)
            {
                string colName = mapping.AllDataColsByObjAttrs[attName];
                Type colType = mapping.DataColTypesByObjAttr[attName];
                if (typeof(IGeometry).IsAssignableFrom(colType))
                {
                    retVal.Sql.Append("SDE.ST_SRID(" + colName +") || ':' || SDE.ST_AsText(");
                    retVal.Sql.Append(colName);
                    retVal.Sql.Append(") AS ");
                    retVal.Sql.Append(colName);
                }
                else
                {
                    if (!colName.Contains("."))
                    {
                        retVal.Sql.Append(mapping.Table);
                        retVal.Sql.Append(".");
                    }
                    retVal.Sql.Append(colName);
                }
                retVal.Sql.Append(", ");
            }
            retVal.Sql.Remove(retVal.Sql.Length - 2, 2);
            retVal.Sql.Append(" FROM ");
            retVal.Sql.Append(mapping.Table);
            ExpressionsToQuery(retVal, crit, mapping);
            OrdersToQuery(retVal, crit, mapping);

            // Don't return the query, we'll do that in DisposeOfQuery.
            return retVal;
        }

        /// <summary>
        /// Since it is implementation-dependent whether to use the sql parameters collection
        /// or not, this method should be implemented in each implementation.
        /// </summary>
        /// <param name="queryToAddTo">Query to add the parameter to.</param>
        /// <param name="columnType">Type of data actually stored in the DB column.  For example,
        ///                          Enums may be stored as strings.  May be null if no type cast
        ///                          is necessary.</param>
        /// <param name="value">Actual value that we need to append to our SQL.</param>
        public override void AppendParameter(SqlDaQuery queryToAddTo, object value,
                                            Type columnType)
        {
            queryToAddTo.Sql.Append("?");
            if (columnType != null)
            {
                value = CoerceType(columnType, value);
            }
            queryToAddTo.Params.Add(value);
        }

        /// <summary>
        /// Overridden to convert IGeometries to WKT and Enums to Int
        /// </summary>
        /// <param name="table">The table these values will be inserted or updated into.</param>
        /// <param name="propValues">A dictionary of "column"/value pairs for the object to insert or update.</param>
        protected override void PreProcessPropertyValues(string table,
                                                         IDictionary<string, object> propValues)
        {
            IDictionary<string, object> propsToModify = new Dictionary<string, object>();
            foreach (string propName in propValues.Keys)
            {
                if (propValues[propName] is IGeometry)
                {
                    IGeometry theGeom = (IGeometry)propValues[propName];
                    propsToModify.Add(propName, _wktWriter.Write(theGeom));
                }
                
                // Attempting to create an OracleCommand object with an Enum instance
                // fails with null reference exception. To get around that, I am casting
                // the enum to an int.
                if (propValues[propName] is Enum)
                {
                    propsToModify.Add(propName, (int)propValues[propName]);
                }
            }
            foreach (string propName in propsToModify.Keys)
            {
                propValues[propName] = propsToModify[propName];
            }
        }

        /// <summary>
        /// By default, the Insert query generated by FastDAO uses "?" as a parameter placeholder.
        /// When inserting into an SDE geometry column, the parameter is a WKT string, so the
        /// placeholder must be wrapped in a SDE.ST_Geometry function call.
        /// </summary>
        /// <param name="table">The table into which a row is being inserted</param>
        /// <param name="propValues">The column names values to be inserted</param>
        /// <returns>A dictionary keyed by column names containing placeholder values
        /// that to be appended on to the insert statement instead of "?"</returns>
        protected override IDictionary<string, string> GetValueStrings(string table, IDictionary<string, object> propValues)
        {
            var valueStrings = new Dictionary<string, string>();
            foreach (string propName in propValues.Keys)
            {
                if (propValues[propName] is IGeometry)
                {
                    var srid = ((IGeometry)propValues[propName]).SRID.ToString(CultureInfo.InvariantCulture);
                    valueStrings.Add(propName, "SDE.ST_Geometry(?, " + srid + ")");
                }
                else
                {
                    valueStrings.Add(propName, "?");
                }
            }
            return valueStrings;
        }

        /// <summary>
        /// Creates a geometry object from the string input.
        /// </summary>
        /// <param name="input">A string of the format {SRID}:{WKT}</param>
        /// <returns>A geometry object</returns>
        public object CreateGeometry(object input)
        {
            if (input == null)
            {
                return null;
            }
            
            var parts = ((string)input).Split(':');
            var srid = int.Parse(parts[0]);
            var wkt = parts[1];
            // For some reason, sde.ST_AsText() is returning well-known text with no type,
            // e.g. "( x1 y1, x2 y2, ...)"
            if (wkt.StartsWith("((("))
            {
                wkt = "multipolygon " + wkt;
            }
            if (wkt.StartsWith("(("))
            {
                wkt = "polygon " + wkt;
            }
            else if (wkt.StartsWith("("))
            {
                wkt = "linestring " + wkt;
            }
            IGeometry geom = (wkt == "EMPTY" ? null : _wktReader.Read(wkt));
            if (geom != null)
            {
                geom.SRID = srid;
            }
            return geom;
        }

        /// <summary>
        /// Converts a single Expression to SQL (mapping the columns as appropriate) and appends
        /// to the given string builder.
        /// 
        /// The expression's SQL will already be wrapped in parens, so you do not need to add them
        /// here.
        /// </summary>
        /// <param name="queryToAddTo">Query we're adding the expression to.</param>
        /// <param name="expr">The expression.  NOTE: It should NOT be null. This method does not check.</param>
        /// <param name="mapping">Class mapping for the class we're dealing with.</param>
        /// <param name="colPrefix">What to prefix column names with, I.E. "Table." for "Table.Column".
        ///                         May be null if no prefix is desired.  May be something other than
        ///                         the table name if the tables are being aliased.</param>
        /// <param name="booleanOperator">The boolean operator (AND or OR) to insert before
        ///                               this expression.  Blank ("") if we don't need one.</param>
        /// <returns>Whether or not this expression modified the sql string.
        ///          Typically true, but may be false for special query types 
        ///          that use other parameters for certain types of expressions.</returns>
        protected override bool ExpressionToQuery(SqlDaQuery queryToAddTo, IExpression expr,
                                                  ClassMapping mapping, string colPrefix, string booleanOperator)
        {
            // All the spatial expressions we support modify the sql.
            bool retVal = true;
            bool trueOrNot = expr.TrueOrNot();
            if (expr is IntersectsExpression)
            {
                queryToAddTo.Sql.Append(booleanOperator);
                IntersectsExpression intersects = (IntersectsExpression)expr;

                const string intersectsFormatString = "SDE.ST_Intersects({0}, {1}) = 1";

                // It is important that the input geometry is the second parameter, otherwise the
                // spatial index does not get used.
                queryToAddTo.Sql.Append(string.Format(intersectsFormatString,
                    colPrefix + mapping.AllDataColsByObjAttrs[intersects.Property],    // Shape column name
                    string.Format("SDE.ST_Geometry(?,{0})", intersects.Shape.SRID))); // geom param converted from WKT
                queryToAddTo.Params.Add(_wktWriter.Write(intersects.Shape));
            }
            else if (expr is WithinExpression)
            {
                queryToAddTo.Sql.Append(booleanOperator);
                WithinExpression within = (WithinExpression)expr;

                const string withinFormatString = "SDE.ST_Within({0}, {1}) = 1";

                // It is important that the input geometry is the second parameter, otherwise the
                // spatial index does not get used.
                queryToAddTo.Sql.Append(string.Format(withinFormatString,
                    colPrefix + mapping.AllDataColsByObjAttrs[within.Property],    // Shape column name
                    string.Format("SDE.ST_Geometry(?,{0})", within.Shape.SRID))); // geom param converted from WKT
                queryToAddTo.Params.Add(_wktWriter.Write(within.Shape));
            }
            else if (expr is ContainsExpression)
            {
                queryToAddTo.Sql.Append(booleanOperator);
                ContainsExpression contains = (ContainsExpression)expr;
                const string containsFormatString = "SDE.ST_Contains({0}, {1}) = 1";

                // It is important that the input geometry is the second parameter, otherwise the
                // spatial index does not get used.
                queryToAddTo.Sql.Append(string.Format(containsFormatString,
                    colPrefix + mapping.AllDataColsByObjAttrs[contains.Property],    // Shape column name
                    string.Format("SDE.ST_Geometry(?,{0})", contains.Shape.SRID))); // geom param converted from WKT
                queryToAddTo.Params.Add(_wktWriter.Write(contains.Shape));
            }
            else if (expr is AbstractDistanceExpression)
            {
                queryToAddTo.Sql.Append(booleanOperator);
                AbstractDistanceExpression dist = (AbstractDistanceExpression)expr;

                queryToAddTo.Sql.Append("SDE.ST_Distance(");
                queryToAddTo.Sql.Append(colPrefix).Append(mapping.AllDataColsByObjAttrs[dist.Property]);
                queryToAddTo.Sql.Append(string.Format("SDE.ST_Contains(SDE.ST_Geometry(?,{0}),", dist.Shape.SRID));
                queryToAddTo.Params.Add(WKTWriter.ToPoint(((IPoint)dist.Shape).Coordinate));
                if (dist is LesserDistanceExpression)
                {
                    queryToAddTo.Sql.Append(trueOrNot ? " < ?" : " >= ?");
                }
                else if (expr is GreaterDistanceExpression)
                {
                    queryToAddTo.Sql.Append(trueOrNot ? " > ?" : " <= ?");
                }
                else
                {
                    throw new ArgumentException("Distance expression type " +
                                                expr.GetType() + " not supported.", "expr");
                }
                queryToAddTo.Params.Add(dist.Distance);
            }
            else if (expr is AbstractDistanceSphereExpression)
            {
                throw new ArgumentException("Distance expression type " +
                                                expr.GetType() + " not supported.", "expr");
            }
            else
            {
                // The expression type does not required special handling by this subclass
                retVal = base.ExpressionToQuery(queryToAddTo, expr, mapping, colPrefix, booleanOperator);
            }
            return retVal;
        }

        #region Implementation of IDaDdlLayer

        /// <summary>
        /// Returns the DDL for the type of an automatically incrementing column.
        /// Some databases only store autonums in one col type so baseType may be
        /// ignored.
        /// </summary>
        /// <param name="baseType">The data type of the column (nominally).</param>
        /// <returns>The autonumber definition string.</returns>
        protected override string GetAutoType(Type baseType)
        {
            throw new NotSupportedException("Oracle does not have autonumbers, use a sequence.");
        }

        /// <summary>
        /// Returns the SQL type used to store a byte array in the DB.
        /// </summary>
        protected override string GetByteArrayType()
        {
            return "BLOB";
        }

        /// <summary>
        /// Returns the SQL type used to store a long in the DB.
        /// </summary>
        protected override string GetLongType()
        {
            return "BIGINT";
        }

        /// <summary>
        /// Returns the SQL type used to store a "normal" (unicode) string in the DB.
        /// </summary>
        protected override string GetStringType()
        {
            return "VARCHAR2(2000)";
        }
        /// <summary>
        /// Oracle doesn't seem to have a varchar type that is limited to ASCII characters.
        /// </summary>
        /// <returns></returns>
        protected override string GetAsciiStringType()
        {
            return "VARCHAR2(2000)";
        }

        /// <summary>
        /// Returns whether a sequence with this name exists or not.
        /// Firebird doesn't appear to support the SQL standard information_schema.
        /// </summary>
        /// <param name="name">Name of the sequence to check for.</param>
        /// <returns>Whether a sequence with this name exists in the data source.</returns>
        public override bool SequenceExists(string name)
        {
            int count = SqlConnectionUtilities.XSafeIntQuery(_connDesc,
                "SELECT count(*) FROM user_sequences WHERE sequence_name = '" +
                name.ToUpper() + "'", null);
            return count > 0;
        }

        /// <summary>
        /// Returns true if you need to call "CreateStoreRoom" before storing any
        /// data.  This method is "Missing" not "Exists" because implementations that
        /// do not use a store room can return "false" from this method without
        /// breaking either a user's app or the spirit of the method.
        /// 
        /// Store room typically corresponds to "table".
        /// </summary>
        /// <returns>Returns true if you need to call "CreateStoreRoom"
        ///          before storing any data.</returns>
        public override bool StoreRoomMissing(ClassMapping mapping)
        {
            // The user_tables doesn't store names with the owner prefix, remove it
            // for the query
            var start = mapping.Table.LastIndexOf('.');

            // -1 is not found, but is not a good index, use 0
            start = start != -1 ? (start + 1) : 0;

            int count = SqlConnectionUtilities.XSafeIntQuery(_connDesc,
                "SELECT COUNT(*) FROM user_tables where table_name = '" +
                mapping.Table.Substring(start).ToUpper() + "'", null);
            return count == 0;
        }

        public override int GetNextSequenceValue(ITransaction transaction, string sequenceName)
        {
            return SqlConnectionUtilities.XSafeIntQuery(_connDesc, (SqlTransaction)transaction,
                                                   _connDesc.MakeSequenceValueQuery(sequenceName), null);
        }

        #endregion
    }
}
