package com.gzgd.utils

object SQLUtils {

	val groupAQueryCondition =
		"""
		  |SELECT
		  |	c.action_time, userNumA, exposureA, clickNumA, orderNumA
		  |FROM
		  |	v_click_a c
		  |LEFT JOIN
		  |	v_exposure_a e
		  |ON
		  |	c.action_time=e.action_time
		  |LEFT JOIN
		  |	v_user_a u
		  |ON
		  |	c.action_time=u.action_time
		  |LEFT JOIN
		  |	v_order_a o
		  |ON
		  |	c.action_time=o.action_time
		  |ORDER BY
		  |	c.action_time
		""".stripMargin

	val groupBQueryCondition =
		"""
		  |SELECT
		  |	c.action_time, userNumB, exposureB, clickNumB, orderNumB
		  |FROM
		  |	v_click_b c
		  |LEFT JOIN
		  |	v_exposure_b e
		  |ON
		  |	c.action_time=e.action_time
		  |LEFT JOIN
		  |	v_user_b u
		  |ON
		  |	c.action_time=u.action_time
		  |LEFT JOIN
		  |	v_order_b o
		  |ON
		  |	c.action_time=o.action_time
		  |ORDER BY
		  |	c.action_time
		""".stripMargin

	val provinceQueryCondition =
		"""
		  |SELECT
		  |	c.action_time, clickNumA, exposureA, userNumA, orderNumA
		  |FROM
		  |	v_click_a c
		  |LEFT JOIN
		  |	v_exposure_a e
		  |ON
		  |	c.action_time=e.action_time
		  |LEFT JOIN
		  |	v_user_a u
		  |ON
		  |	c.action_time=u.action_time
		  |LEFT JOIN
		  |	v_order_a o
		  |ON
		  |	c.action_time=o.action_time
		  |ORDER BY
		  |	c.action_time
		""".stripMargin

	val positionQueryCondition =
		"""
		  |select
		  |	t.rec_posi,
		  | aae.exposure exposure_ai_a, aac.click click_ai_a, aao.order order_ai_a,
		  | ame.exposure exposure_man_a, amc.click click_man_a, amo.order order_man_a,
		  | bae.exposure exposure_ai_b, bac.click click_ai_b, bao.order order_ai_b,
		  | bme.exposure exposure_man_b, bmc.click click_man_b, bmo.order order_man_b
		  |from
		  |	v_table t
		  |left join v_a_ai_exposure aae
		  |on t.rec_posi=aae.rec_posi
		  |left join v_a_ai_click aac
		  |on t.rec_posi=aac.rec_posi
		  |left join v_a_ai_order aao
		  |on t.rec_posi=aao.rec_posi
		  |left join v_a_man_exposure ame
		  |on t.rec_posi=ame.rec_posi
		  |left join v_a_man_click amc
		  |on t.rec_posi=amc.rec_posi
		  |left join v_a_man_order amo
		  |on t.rec_posi=amo.rec_posi
		  |left join v_b_ai_exposure bae
		  |on t.rec_posi=bae.rec_posi
		  |left join v_b_ai_click bac
		  |on t.rec_posi=bac.rec_posi
		  |left join v_b_ai_order bao
		  |on t.rec_posi=bao.rec_posi
		  |left join v_b_man_exposure bme
		  |on t.rec_posi=bme.rec_posi
		  |left join v_b_man_click bmc
		  |on t.rec_posi=bmc.rec_posi
		  |left join v_b_man_order bmo
		  |on t.rec_posi=bmo.rec_posi
		""".stripMargin

}
