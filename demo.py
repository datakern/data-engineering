class SchoolApplication():
    def __init__(self, name, age, city):
        self.name = name
        self.__age = age
        self.city = city
    
    def validate_age(self):
        if self.__age <=0 or self.__age >=100:
            print("Enter the valid age number")

class ScholorshipApplication(SchoolApplication):
    def __init__(self, name, age, city, gpa):
        super().__init__(name, age, city)
        self.gpa = gpa
    
    def check_scholorship_eligibility(self):
        if self.__age >=18 and self.gpa >=4:
            print("Student is eligible for scholorship")
        else:
            print("Student is not eligible for scholorship")
    

kiran = SchoolApplication("Kiran", 38, "Bangalore")

print(kiran.__age)
kiran.validate_age()
# kiran.check_scholorship_eligibility()



    
