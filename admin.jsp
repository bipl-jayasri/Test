<%@page import="java.util.Vector"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<%@ include file="../util/common_js.jsp"%>
<script>
function setValue(obj,user_cd)
{
	window.opener.refreshChild(user_cd);
	window.close();
}
</script>
</head>
<jsp:useBean class="com.etrm.fms.admin.DataBean_Admin" id="dbadmin" scope="page"></jsp:useBean>
<jsp:useBean class="com.etrm.fms.util.MessageUtil" id="utilmsg" scope="request"></jsp:useBean>
<jsp:useBean class="com.etrm.fms.util.DateUtil" id="utildate" scope="request"></jsp:useBean>
<%
String opration=request.getParameter("opration")==null?"INSERT":request.getParameter("opration");

dbadmin.setCallFlag("USER_LIST");
dbadmin.setComp_cd(owner_cd);
dbadmin.setOpration(opration);
dbadmin.init();

Vector VEMP_CD = dbadmin.getVEMP_CD();
Vector VEMP_NM = dbadmin.getVEMP_NM();
Vector VEMP_UID = dbadmin.getVEMP_UID();
%>
<body>
<form action="">

<div class="box-body">
	<div class="row">
		<div class="col-md-12 col-sm-12 col-xs-12">
			<div class="card cardmain">
				<div class="card-header cdheader">
					<div class="d-flex justify-content-between">
						<div class="topheader">
						<%if(opration.equals("INSERT")){ %>
							Add/Import User
						<%}else{ %>
							User List
						<%} %>
						</div>
					</div>
				</div>
				<div class="card-body cdbody">
					<div class="row">
						<div class="table-responsive">
							<table class="table table-bordered table-hover" id="example">
								<thead>
									<tr>
										<th>Select</th>
										<th>
											User Name
											<br><div align="center"><input class="form-control form-control-sm" type="text" id="table_userNm" onkeyup="Search(this,'1');" placeholder="Search.." style="width:100px"/></div>
										</th>
										<th>
											User ID
											<br><div align="center"><input class="form-control form-control-sm" type="text" id="table_userId" onkeyup="Search(this,'2');" placeholder="Search.." style="width:100px"/></div>
										</th>
									</tr>
								</thead>
								<tbody>
								<%if(VEMP_CD.size() > 0){ %>
									<%for(int i=0;i<VEMP_CD.size();i++){ %>
									<tr>
										<td width="5px" align="center">
											<input type="radio" name="rdo" onclick="setValue(this,'<%=VEMP_CD.elementAt(i)%>')">
										</td>
										<td><font color="<%if(VEMP_CD.elementAt(i).equals("0")){%>blue<%}%>"><%=VEMP_NM.elementAt(i)%></font></td>
										<td><%=VEMP_UID.elementAt(i)%></td>
									</tr>
									<%} %>
								<%}else{ %>
									<tr>
										<td colspan="2">
											<%=utilmsg.infoMessage("<b>No User Configured!</b>") %>
										</td>
									</tr>
								<%} %>
								</tbody>
							</table>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>	

<input type="hidden" name="read_access" value="<%=read_access%>">
<input type="hidden" name="write_access" value="<%=write_access%>">
<input type="hidden" name="check_access" value="<%=check_access%>">
<input type="hidden" name="print_access" value="<%=print_access%>">
<input type="hidden" name="delete_access" value="<%=delete_access%>">  	
<input type="hidden" name="audit_access" value="<%=audit_access%>">
<input type="hidden" name="authorize_access" value="<%=authorize_access%>">
<input type="hidden" name="approve_access" value="<%=approve_access%>">
<input type="hidden" name="execute_access" value="<%=execute_access%>">
<input type="hidden" name="form_cd" value="<%=formCd%>">
<input type="hidden" name="form_nm" value="<%=formNm%>">
<input type="hidden" name="mod_cd" value="<%=mod_cd%>">
<input type="hidden" name="mod_nm" value="<%=mod_nm%>">

</form>
</body>
<script>
function Search(obj, indx) 
{
  	var input, filter, table, tr, td, i, txtValue,count=0;
  	input = document.getElementById(obj.id);
  	
  	filter = input.value.toLocaleLowerCase();
  	table = document.getElementById("example");
  	
  	tr = table.getElementsByTagName("tr");
  	for (i = 1; i < tr.length; i++) 
  	{
    	td = tr[i].getElementsByTagName("td")[indx];
    	if (td) 
    	{
      		txtValue = td.textContent || td.innerText;
      		if (txtValue.toLocaleLowerCase().indexOf(filter) > -1) {
        		tr[i].style.display = "";
        		count++;
      		} else {
      			tr[i].style.display = "none";
      		}
    	}       
  	}
}
</script>
</html>
