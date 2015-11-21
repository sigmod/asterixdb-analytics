use dataverse graph;

drop dataset MsgGraph2 if exists;

drop dataset results if exists;

create temporary dataset MsgGraph2(VertexType) primary key vertexid;

create temporary dataset results(Result) primary key vertexid;