explain extended
 select * from (select a.key as ak, a.value as av, b.key as bk, b.value as bv from src a join src1 b where a.key = '429' ) c order by c.ak, c.av, c.bk, c.bv;

 select * from (select a.key as ak, a.value as av, b.key as bk, b.value as bv from src a join src1 b where a.key = '429' ) c order by c.ak, c.av, c.bk, c.bv;
