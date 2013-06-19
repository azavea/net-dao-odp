using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azavea.Open.Common;
using Azavea.Open.DAO.SQL;
using Azavea.Open.DAO.Tests;
using NUnit.Framework;

namespace Azavea.Open.DAO.Odp.Tests
{
    /// <exclude/>
    [TestFixture]
    public class OdpTests : AbstractFastDAOTests
    {
        public OdpTests() : base(new Config("..\\..\\Tests\\dao.config", "DaoConfig"), "NonSpatialDao",
            false, true, false, true, false, true) { }
    }
}
