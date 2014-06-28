package nosql.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Random;

public class RandomUtils {

	private static Random r = new Random(System.currentTimeMillis());

	private static <T> T randomObject(RandomObjectFunction<T> f) {
		T t = null;
		do {
			if (r.nextInt(10) != 0) {
				t = f.get();
			}
		} while (t == null);
		return t;
	}

	public static Date randomDate() {
		return randomObject(new RandomObjectFunction<Date>() {
			public Date get() {
				long offset = Timestamp.valueOf("2010-01-01 00:00:00")
						.getTime();
				long end = Timestamp.valueOf("2013-04-30 00:00:00").getTime();
				long diff = end - offset + 1;
				Timestamp rand = new Timestamp(offset
						+ (long) (r.nextDouble() * diff));
				return new Date(rand.getTime());
			}
		});
	}

	public static Double randomDouble(final int numberOfDigits,
			final int decimalDigits) {
		return randomObject(new RandomObjectFunction<Double>() {
			public Double get() {
				double digits = (int) (Math.pow(10d, numberOfDigits)
						* r.nextDouble() - 1.0d);
				return digits / Math.pow(10.0d, decimalDigits);
			}
		});
	}
	
	public static String randomString(final int size) {
		return randomObject(new RandomObjectFunction<String>() {
			public String get() {
				StringBuilder sb = new StringBuilder(size);
				for (int i = 0; i < size; i++) {
					char c = (char) ((r.nextBoolean() ? 'A' : 'a') + r
							.nextInt('z' - 'a'));
					sb.append(c);
				}
				return sb.toString();
			}
		});
	}

	public static Integer randomInt() {
		return randomObject(new RandomObjectFunction<Integer>() {
			public Integer get() {
				return 1000 + r.nextInt(Integer.MAX_VALUE - 1000);
			}
		});
	}
}
