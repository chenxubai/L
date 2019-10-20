package gson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class TestGson {

	public static void main(String[] args) {
		Type listType = new TypeToken<Test>() {}.getType();
		Gson gson = new GsonBuilder().create();
		List<Test> l = new ArrayList<Test>();
		l.add(new Test("bobo", "34"));
				
		System.out.println(gson.toJson(new Test("bobo", "34"), listType));;
	}
	
	static class Test {
		private String name ;
		private String age ;
		
		public Test(String name, String age) {
			this.name = name;
			this.age = age;
		}
	}

}
