class SchoolApplicationForm:
    def __init__(self,student_name,dob,city):
        self.student_name = student_name
        self.dob = dob
        self.city = city

    def get_eligibility(self):
        if self.city == "Hyd":
            print("Eligible for school")
        else:
            print("Not eligible for school")
        


kiran = SchoolApplicationForm("kiran", "2000-01-01", "Hyd")

def main():
    kiran.get_eligibility()
    print(kiran.student_name)
    print(kiran.dob)
    print(kiran.city)


if __name__ == "__main__":
    main()

            
        


