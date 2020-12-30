using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Data.SqlClient;
using System.Net.Http;
using System.Web.Http;
using System.Data.Sql;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using Newtonsoft.Json;
using SH3H.BZ.EntityFrameWork;

namespace zz_bigdata_api.Controllers
{
   
    public class big_dataController : ApiController
    {
        public IEnumerable<string> GetInfo(string invoice_code,string card_name)
        {
            OracleParameter[] param = new OracleParameter[]{

            new OracleParameter(":invoice_code",OracleDbType.Varchar2,50, ParameterDirection.Input),
            new OracleParameter(":card_name",OracleDbType.Varchar2,50, ParameterDirection.Input),
            new OracleParameter("RESULTS",OracleDbType.RefCursor, ParameterDirection.Output),
        };
            param[0].Value = invoice_code;
            param[1].Value = card_name;
            string sql = "zz_bigdata";
          //  string sql1 = "select fp.invoice_name as 企业名称, fp.invoice_code as 统一社会信用代码,fp.card_id as 用户编号,case bk.user_type  when 1 then  '非居民'when 0 then     '居民'end as 用户类型, null as 服务点编号,  '郑州市' as 企业所在市,  null as 企业所在区,   bk.card_address as 企业地址,  fp.card_id as 表号,  null as 用水水量,  null as 用水费用时间,       null as 用水金额, null as 欠费费用类型,null as 用水费用时间, sum(yyz.acc_money) as 欠费金额  from cm_invoice_setting fp  left  join cm_metercards bk  on bk.card_id = fp.card_id  left  join acc_charges yyz on yyz.card_id = fp.card_Id where nvl(fp.invoice_code, '00') <> '00'   and yyz.pay_state = 0    and fp.invoice_code = \'" + invoice_code + "\' group by fp.invoice_name,fp.invoice_code,fp.card_id,bk.user_type,bk.card_address";
         //   string sql = "select acc_water,acc_money,bk.card_name from acc_charges yyz inner join cm_metercards bk on bk.card_id=yyz.card_id where card_name=\'" + card_name + "\' and bk.card_id=\'" + card_id + "\'";
            DataTable dt = new DataTable();
            dt = OricalHelper.ExecuteDataTable(sql, true, param);
            string json=DataTableToJsonWithJsonNet(dt);
            yield return json;
        }

       
        public string DataTableToJsonWithJsonNet(DataTable table)
        {
            string JsonString = string.Empty;
            JsonString = JsonConvert.SerializeObject(table);
            return JsonString;
        }


    }
}
