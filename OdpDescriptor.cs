using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text;
using Azavea.Open.Common;
using Azavea.Open.DAO.SQL;
using Azavea.Open.DAO.Util;
using Oracle.DataAccess.Client;

namespace Azavea.Open.DAO.Odp
{
    public class OdpDescriptor : AbstractSqlConnectionDescriptor, ITransactionalConnectionDescriptor
    {
        /// <summary>
        /// The server name, meaningful for some databases (Oracle, SQL Server) but not others (Access).
        /// May be null depending on the database.
        /// </summary>
        public readonly string Server;

        /// <summary>
        /// The user name, if necessary to log into the database.  May be null.
        /// </summary>
        public readonly string User;

        /// <summary>
        /// The password for the User.  May be null.
        /// </summary>
        public readonly string Password;

        protected readonly string ConnectionString;
        protected readonly string CleanConnectionString;

        /// <summary>
        /// The connection timeout, in seconds.  May be null, meaning use the default.
        /// </summary>
        public readonly int? ConnectTimeout;

        public OdpDescriptor(Config config, string component,
            ConnectionInfoDecryptionDelegate decryptionDelegate)
            : this(config.GetParameter(component, "Server", null),
                   config.GetParameter(component, "User", null),
                   GetDecryptedConfigParameter(config, component, "Password", decryptionDelegate),
                   config.GetParameterAsInt(component, "Connect_Timeout", null)) {}

        protected OdpDescriptor(string server, string user, string password, int? connectionTimeout)
        {
            Server = server;
            User = user;
            Password = password;
            ConnectTimeout = connectionTimeout;
            ConnectionString = MakeConnectionString(server, user, password, connectionTimeout);
            CleanConnectionString = MakeConnectionString(server, user, null, connectionTimeout);
        }

        /// <summary>
        /// Assembles a connection string that can be used to get a database connection.
        /// All the parameters are optional for the purposes of this method, although obviously
        /// it would be possible to create a useless connection string if you leave out important
        /// parameters.
        /// </summary>
        /// <param name="server">Server name that is hosting the database</param>
        /// <param name="user">User name to use when accessing the db</param>
        /// <param name="password">Password for above user.</param>
        /// <param name="connectionTimeout">How long to wait before giving up on a command, in seconds.</param>
        /// <returns>A connection string that can be used to create ODP.NET connections.</returns>
        public static string MakeConnectionString(string server, string user, string password, int? connectionTimeout)
        {
            var connStringBuilder = new StringBuilder();

            if (StringHelper.IsNonBlank(server))
            {
                connStringBuilder.Append("Data Source=" + server + ";");
            }
            if (connectionTimeout != null)
            {
                 connStringBuilder.Append("Connect Timeout=" + connectionTimeout + ";");
            }
            if (StringHelper.IsNonBlank(user))
            {
                 connStringBuilder.Append("User ID=" + user + ";");
            }
            if (StringHelper.IsNonBlank(password))
            {
                 connStringBuilder.Append("Password=" + password);
            }
            return connStringBuilder.ToString();
        }

        public override string ToCompleteString()
        {
            return ConnectionString;
        }

        public override string ToCleanString()
        {
            return CleanConnectionString;
        }

        public ITransaction BeginTransaction()
        {
            return new SqlTransaction(this);
        }

        public override bool UsePooling()
        {
            // As I understand it, ODP handles its own pooling.
            return false;
        }

        public override DbConnection CreateNewConnection()
        {
            return new OracleConnection(ToCompleteString());
        }

        public override void SetParametersOnCommand(IDbCommand cmd, IEnumerable parameters)
        {
            var placeholders = SqlUtilities.FindParameterPlaceholders(cmd.CommandText);
            var updatedCommandTextBuilder = new StringBuilder(cmd.CommandText);
            int textPositionOffset = 0;
            int x = 0;
            foreach (object param in parameters)
            {
                if (x >= placeholders.Length)
                {
                    throw new ArgumentException("Only " + placeholders.Length + " parameter placeholders were found in the command but >=" + x + " parameter values were specified. CommandText: " + cmd.CommandText);
                }

                object addMe;
                if (param == null)
                {
                    addMe = DBNull.Value;
                }
                else if (param is Enum)
                {
                    // Attempting to create an OracleCommand object with an Enum instance
                    // fails with null reference exception. To get around that, I am casting
                    // the enum to an int.
                    addMe = (int)param;
                }
                else
                {
                    addMe = param;
                }

                string paramName = DbCaches.ParamNames.Get(x);
                
                OracleParameter oracleParam;
                if (addMe != DBNull.Value && addMe is string && ((string)addMe).Length > 3999)
                {
                    // TODO: ensure the param is passed as a geom and convert to WKT here
                    // Checking string length to trigger CLOB creation is a hack.
                    oracleParam = new OracleParameter(paramName, OracleDbType.Clob, ((string)addMe).Length, addMe, ParameterDirection.Input);
                }
                else
                {
                    oracleParam = new OracleParameter(paramName, addMe);
                }
                cmd.Parameters.Add(oracleParam);

                updatedCommandTextBuilder.Replace("?", ":" + paramName, placeholders[x] + textPositionOffset, 1);
                textPositionOffset += paramName.Length;
                x++;
            }
            cmd.CommandText = updatedCommandTextBuilder.ToString();
        }

        public override string MakeSequenceValueQuery(string sequenceName)
        {
            StringBuilder sb = DbCaches.StringBuilders.Get();           
            sb.Append("SELECT ");
            sb.Append(sequenceName);
            sb.Append(".NEXTVAL FROM DUAL");           
            string retVal = sb.ToString();
            DbCaches.StringBuilders.Return(sb);
            return retVal;
        }

        public override DbDataAdapter CreateNewAdapter(IDbCommand cmd)
        {
            return new OracleDataAdapter(cmd as OracleCommand);
        }

        public override bool NeedToAliasColumns()
        {
            return true;
        }

        public override bool CanUseAliasInOrderClause()
        {
            return true;
        }

        public override bool NeedAsForColumnAliases()
        {
            return false;
        }

        public override string ColumnAliasPrefix()
        {
            return "\"";
        }

        public override string ColumnAliasSuffix()
        {
            return "\"";
        }

        public override string TableAliasPrefix()
        {
            return "";
        }

        public override string TableAliasSuffix()
        {
            return "";
        }

        /// <exclude/>
        public override SqlClauseWithValue MakeModulusClause(string fieldName)
        {
            StringBuilder sb = DbCaches.StringBuilders.Get();
            SqlClauseWithValue retVal = DbCaches.Clauses.Get();
            sb.Append("MOD(");
            sb.Append(fieldName);
            sb.Append(", ");
            retVal.PartBeforeValue = sb.ToString();
            retVal.PartAfterValue = ")";
            DbCaches.StringBuilders.Return(sb);
            return retVal;
        }
    }
}
