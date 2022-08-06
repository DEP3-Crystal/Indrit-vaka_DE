package crystal.training.exercises;
// lombok
public class Person_Build
{
  public static class Address {
    private final String country;
    private final String city;

    public Address(String country, String city) {
      this.country = country;
      this.city = city;
    }

    public String getCountry() {
      return country;
    }

    public String getCity() {
      return city;
    }

    public static Builder builder() {
      return new Builder();
    }

    public Builder toBuilder() {
      return new Builder().city(city).country(country);
    }

    private static class Builder {
      private String country;
      private String city;

      public Builder country(String country) {
        this.country = country;
        return this;
      }

      public Builder city(String city) {
        this.city = city;
        return this;
      }

      public Address build() {
        return new Address(country, city);
      }
    }
  }

  private String name;
  private int age;
  private Address address;

  private Person_Build(Builder builder){
    this.name = builder.name;
    this.age = builder.age;
    this.address = builder.address;
  }
  public static Builder builder(){
    return new Builder();
  }
  private static class Builder {

    private String name;
    private int age;
    private Address address;

    public Builder name(String name){
      this.name = name;
      return this;
    }
    public Builder age(int age){
      this.age = age;
      return this;
    }
    public Builder address(Address address){
      this.address = address;
      return this;
    }
    public Person_Build build() {
      return new Person_Build(this);
    }
  }

  public static void main(String[] args) {
    Address add1 = new Address("London", "GB");
    Address add2 = Address.builder().country("London")
            .city("GB").build();
    Address add3 = add2.toBuilder()
            .city("Birmingham")
            ////
            .build();
    // TO BE COMPLETED BY YOU exercises
    Person_Build person = Person_Build.builder().name("Indrit").address(add1).age(21).build();
    System.out.println(person.name);
    //Changes
    //  1. Creating the builder for the person
    //  2. Crating the Person Obj
  }
}