CALL apoc.do.case(
    [
        conditional1,
        "query1",
        condiational2,
        "query2",
        conditionalN,
        "queryN"
    ],
    "elseQuery",
    {
        paramKeyFromInnerScope:paramValueFromOuterScope,
        paramKeyFromInnerScope:paramValueFromOuterScope,
        ...
    }
)
YIELD value
RETURN value AS resultMap