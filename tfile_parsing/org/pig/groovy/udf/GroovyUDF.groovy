import org.apache.pig.builtin.OutputSchema;

class GroovyUDFs {
    @OutputSchema('x:long')
    long square(long x) {
        return x * x;
    }
}