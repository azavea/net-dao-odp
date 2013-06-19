using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using Azavea.Open.Common;
using Azavea.Open.DAO.Tests;
using NUnit.Framework;

namespace Azavea.Open.DAO.Odp.Tests
{
    [TestFixture]
    public class OdpDescriptorTests
    {
        /// <exclude/>
        [Test]
        public void QuestionMarksAreReplacedWithOracleCompatiblePlaceholders()
        {
            var descriptor = (OdpDescriptor)ConnectionDescriptor.LoadFromConfig(
                        new Config("..\\..\\Tests\\dao.config", "DaoConfig"), "NonSpatialDao");
            const string initialSql = "SELECT * FROM TABLE WHERE COL = ? AND COL2 = ?";
            const string expectedSql = "SELECT * FROM TABLE WHERE COL = :param0 AND COL2 = :param1";
            var command = new TestCommand(initialSql);
            descriptor.SetParametersOnCommand(command, new [] {"foo", "bar"});
            Assert.AreEqual(expectedSql, command.CommandText);
            Assert.AreEqual(2, command.Parameters.Count, "Expected 2 parameters to be created on the command.");
            AssertDbParameter(command.Parameters[0], "param0", "foo");
            AssertDbParameter(command.Parameters[1], "param1", "bar");
        }

        private void AssertDbParameter(DbParameter parameter, string name, string value)
        {
            Assert.AreEqual(parameter.ParameterName, name, "The parameter name does not match the expected name.");
            Assert.AreEqual(parameter.Value, value, "The parameter value does not match the expected name.");
        }
    }
}
