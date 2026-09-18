class SchoolApplicationForm():
    def __init__(self,name,age,city):
        self.name = name
        self.age = age
        self.city = city
    
    def validate_age(self):
        if self.age <=0 or self.age >100:
            print("Enter valid age number")

    def check_eligibility(self):
        if self.age <=15:
            print("Student is eligible for joining the school")
        else:
            print("Student is not eligible for joining the school")

class ScholorshipApplication(SchoolApplicationForm):
    def __init__(self, name, age, city, gpa):
        super().__init__(name,age,city)
        self.gpa = gpa
        self.validate_age()

    def check_eligibility(self):
        if self.age >=20 and self.gpa >= 4:
            print("Student is eligible for scholorship")
        else:
            print("Student is not eligible for scholorship")   

#kiran_school = SchoolApplicationForm("kiran", 20, "Hyderabad")
kiran_scholorship = ScholorshipApplication("kiran",0,"Hyd",5)

def main():
    # kiran_school.check_eligibility()
    kiran_scholorship.check_eligibility()
    # print(kiran_school.age)
    print(kiran_scholorship.age)
    # print(kiran_school.city)
    print(kiran_scholorship.city)

if __name__ == "__main__":
    main()



