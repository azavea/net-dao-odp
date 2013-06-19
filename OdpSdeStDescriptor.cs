using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Azavea.Open.Common;
using Azavea.Open.DAO.OleDb;
using Azavea.Open.DAO.SQL;
using Azavea.Open.DAO.Util;
using Oracle.DataAccess.Client;

namespace Azavea.Open.DAO.Odp
{
    public class OdpSdeStDescriptor : OdpDescriptor
    {

        public OdpSdeStDescriptor(Config config, string component, ConnectionInfoDecryptionDelegate decryptionDelegate)
            : base(config.GetParameter(component, "Server", null),
                   config.GetParameter(component, "User", null),
                   GetDecryptedConfigParameter(config, component, "Password", decryptionDelegate),
                   config.GetParameterAsInt(component, "Connect_Timeout", null)) {}

        public override IDaLayer CreateDataAccessLayer()
        {
            return new OdpSdeStDaLayer(this);
        }

        /// <summary>
        /// Since different databases have different ideas of what a sequence is, this
        /// allows the utility class to support sequences across all different DBs.
        /// </summary>
        /// <param name="sequenceName">The name of the sequence we're getting an ID from.</param>
        /// <returns>A sql string that will retrieve a sequence integer value (I.E. something like
        ///          "SELECT NEXTVAL FROM sequenceName")</returns>
        public override string MakeSequenceValueQuery(string sequenceName)
        {
            StringBuilder sb = DbCaches.StringBuilders.Get();
            //
            // SDE has it's own mechanism for managing sequential IDs. This article
            // explains how they are managed when inserting rows via ST_Geometry
            // http://support.esri.com/es/knowledgebase/techarticles/detail/32657
            //
            // Use the same config file syntax as you would for a non-spatial
            // Oracle sequence, but use OWNER.TABLENAME for the sequence param
            // value.
            //
            // <generator class="sequence">
            //     <param name="sequence">OWNER.TABLENAME</param>
            // </generator>
            //
            var ownerAndName = sequenceName.Split('.');
            var owner = ownerAndName[0];
            var table = ownerAndName[1];
            sb.Append(string.Format("SELECT sde.version_user_ddl.next_row_id('{0}', (SELECT registration_id FROM sde.table_registry WHERE table_name = '{1}' and owner = '{0}')) FROM DUAL", owner, table));
           
            string retVal = sb.ToString();
            DbCaches.StringBuilders.Return(sb);
            return retVal;
        }
    }
}
