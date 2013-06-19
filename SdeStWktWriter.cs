using System;
using System.Text.RegularExpressions;
using GeoAPI.Geometries;
using NetTopologySuite.IO;

namespace Azavea.Open.DAO.Odp
{
    public class SdeStWktWriter : WKTWriter
    {
        public override string Write(IGeometry geometry)
        {
            var wkt = base.Write(geometry);
            if (geometry is IMultiPoint)
            {
                /* There are 2 acceptable OGC WKT representations for MultiPoint
                 * 
                 *   MULTIPOINT((0 1), (2 3))
                 *   MULTIPOINT(0 1, 2 3)
                 *   
                 * The SDE ST_GeomFromText function will only accept the version
                 * without the extra parens, but the NTS WktWriter generates
                 * the version _with_ extra parens.
                 * 
                 * This regex replace strips out the extra parens.
                 */
                return Regex.Replace(wkt, "(MULTIPOINT\\s*\\()(.+)(\\))", match => 
                    match.Groups[1].Value +
                    match.Groups[2].Value.Replace("(", "").Replace(")", "") +
                    match.Groups[3].Value
                );
            }
            else
            {
                return wkt;
            }                
        }

        public override string WriteFormatted(IGeometry geometry)
        {
            throw new NotImplementedException("SdeStWktWriter only supports Write(IGeometry geometry)");
        }

        public override void Write(IGeometry geometry, System.IO.TextWriter writer)
        {
            throw new NotImplementedException("SdeStWktWriter only supports Write(IGeometry geometry)");
        }

        public override void WriteFormatted(IGeometry geometry, System.IO.TextWriter writer)
        {
            throw new NotImplementedException("SdeStWktWriter only supports Write(IGeometry geometry)");
        }
    }
}
