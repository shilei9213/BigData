package x.bd.hadoop.join.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 可标记的结果
 * 
 * @author shilei
 *
 */
public class TaggedValue implements Writable {
	private Text tag;
	private Writable data;

	public TaggedValue() {
		tag = new Text();
	}
	
	public TaggedValue(Writable data) {
		tag = new Text();
		this.data = data;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// 写出内容
		this.tag.write(out);
		out.writeUTF(this.data.getClass().getName());
		this.data.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag.readFields(in);
		String dataClz = in.readUTF();
		try {
			if (this.data == null || !this.data.getClass().getName().equals(dataClz)) {
				this.data = (Writable) ReflectionUtils.newInstance(Class.forName(dataClz), null);
			}
			this.data.readFields(in);
		} catch (ClassNotFoundException cnfe) {
			System.out.println("Problem in TaggedWritable class, method readFields.");
		}
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

	public Writable getData() {
		return data;
	}

	public void setData(Writable data) {
		this.data = data;
	}

	/**
	 * clone克隆 一个 对象数据
	 * 
	 * @param conf
	 * @return
	 */
	public TaggedValue clone(Configuration conf) {
		return (TaggedValue) WritableUtils.clone(this, conf);
	}

	public static void main(String[] args) {
		System.out.println(TaggedValue.class.getName());
	}

}
