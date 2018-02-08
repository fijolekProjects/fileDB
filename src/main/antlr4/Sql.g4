grammar Sql;

select
: K_SELECT result_column
  K_FROM table_name
  (K_WHERE expr) ?
;

create_index
: K_CREATE K_INDEX index_name K_ON table_name '(' column_name ')'
;

create_table
: K_CREATE K_TABLE table_name
'(' column_def ( ',' column_def )* ')'
;

insert
: K_INSERT K_INTO table_name '(' column_name ( ',' column_name )* ')'
  K_VALUES
  '(' literal_value ( ',' literal_value)* ')'
;

result_column
 : '*'
 | table_name '.' '*'
;

expr
 : literal_value
 | column_name
 | expr ( '=' | '!=') expr
;

index_name
 : any_name
;

table_name
 : any_name
;

column_def
 : column_name type_name
;

type_name
 : name+ ( '(' NUMERIC_LITERAL ')' )?
;

name
: any_name
;

column_name
 : any_name
;

any_name
 : IDENTIFIER
 | STRING_LITERAL
;

literal_value
 : NUMERIC_LITERAL
 | STRING_LITERAL
;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
;

K_VALUES : V A L U E S;
K_INSERT : I N S E R T;
K_INTO : I N T O;
K_ON : O N;
K_INDEX : I N D E X;
K_TABLE : T A B L E;
K_CREATE : C R E A T E;
K_SELECT : S E L E C T;
K_FROM : F R O M;
K_WHERE : W H E R E;

fragment DIGIT : [0-9];

IDENTIFIER
 : [a-zA-Z_] [a-zA-Z_0-9]*
;

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
;