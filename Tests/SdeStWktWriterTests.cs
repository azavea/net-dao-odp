using NUnit.Framework;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Azavea.Open.DAO.Odp.Tests
{
    [TestFixture]
    public class SdeStWktWriterTests
    {
        private WKTWriter _originalWktWriter;
        private SdeStWktWriter _sdeStWktWriter;

        [SetUp]
        public void SetUp()
        {
            _originalWktWriter = new WKTWriter();
            _sdeStWktWriter = new SdeStWktWriter();
        }

        /// <exclude/>
        [Test]
        public void MultiPoint()
        {
            var geom = new MultiPoint(new [] { new Point(0,1), new Point(2,3) });

            var actualOriginalWkt = _originalWktWriter.Write(geom);
            Assert.AreEqual("MULTIPOINT ((0 1), (2 3))", actualOriginalWkt, "If the original writer does not include extra parens around the points, consider removing the SdeStWktWriter class.");

            var actualSdeStWkt = _sdeStWktWriter.Write(geom);
            Assert.AreEqual("MULTIPOINT (0 1, 2 3)", actualSdeStWkt);
        }
    }
}
