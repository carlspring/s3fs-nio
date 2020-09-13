# Coding convention

## Reasoning

It is important to have consistency across the codebase. This won't necessarily make your code work better, but it might 
help make it more understandable, less time-consuming and less irritating to go through when doing a code review of your
changes.  
  
While to some this will seem like a nuisance, with no real benefit to the actual code, and while we do understand 
this point of view, we think that reading `diff`-s should be quick and easy. Accepting a pull request requires for it to
be code reviewed first which, in term, means going over the code and figuring out what is what, why the change makes
sense and whether, or not it will impact something else. All of this takes time, which, of course, is a commodity.

## Formatting Rules to Follow

* Use four spaces, instead of a tab.
* Make sure files have an empty line at the end.
* Within methods, please try to leave lines between logical units of code to improve readability. 

    ??? success "Acceptable and preferred"

        ```     
        int i = 5;
        int j = 6;
        int k += i + j + 7;
         
        logger.debug("k = " + k);
         
        k += i + j + 7;
        
        logger.debug("k = " + k);
        
        try
        {
            doSomethingThatMightThrowAnException(k);
        }
        catch (TheException e)
        {
            logger.error(e.getMessage(), e);
        }
        
        logger.debug("k = " + k);
        ```

    ??? danger "Please avoid writing code like this"

        ```
        int i = 5;
        int j = 6;
        int k = 7;
        logger.debug("k = " + k);
        k += i + j + 7;
        logger.debug("k = " + k);
        try
        {
            doSomethingThatMightThrowAnException(k);
        }
        catch (TheException e)
        {
           logger.error(e.getMessage(), e);
        }
        logger.debug("k = " + k);
        ```

* When re-indenting code, please make a single commit with just the indentation changes and make sure you describe that 
  this is just an re-indentation change in the commit message. Mixing reformatting and actual functional changes in the 
  same commit makes things much more obscure to track and figure out.

* Don't reformat entire files, unless absolutely necessary! This makes it harder (and more time-consuming) to check what 
  changes you've actually made.

* Try not to re-order code imports. Sometimes, while optimizing imports this is not possible, but re-ordering a long 
  list of imports can make a diff hard to read.

## Code Example

Please, consider the following an example of how to indent your code.

??? success "Example class with proper formatting"

    ```
    package org.carlspring.cloud.storage.s3fs;
    
    /**
     * License header goes here.
     */
    import com.foo.*;
    import com.foo.dot.*;
    import java.util.*;
    
    /**
     * @author john.doe
     */
    public class Foo extends TestCase
        implements Serializable
    {
        
        private int[] X = new int[]{ 1, 3, 5
                                     7, 9, 11 };
        
        private String[] strings = new String[] { "Some text",
                                                  "Some other text",
                                                  "And a lot more text" };
        
        // For short values:
        @FancyAnnotation(someArrayParameter = { "foo", "bar" })
        private FancyAnnotatedField f1;
        
        // Or... for longer values -- a value per line:
        @FancyAnnotation(someArrayParameter = { "fooLongFooBarBlahFooBlahFoo",
                                                "barFooBlahBlahBlahMoreBlahFoo" })
        private FancyAnnotatedField f2;
        
        
        public void setUp()
        {
            super.setUp();
        }
        
        public void methodWithAnnotations(@SomeAnnotation1
                                          @SomeOtherAnnotation(overide = true,
                                                               doThatOtherThing = true)
                                          MyFancyObject foo
        {
            logger.debug("Something is happening...");
            
            doThis();
            
            logger.info("We did this!");
            logger.info("And it worked!");
            
            int i = 5;
            
            logger.debug("i = " + i);
            
            i++;
            
            logger.debug("i = " + i);
            
            foo.setNumberOfThings(i);
        }
        
        public void test(boolean a,
                         int x,
                         int y,
                         int z)
                throws Exception
        {
            label1:
            do
            {
                try
                {
                    if (x > 0)
                    {
                        int someVariable = x + y == z ?
                                           x + 1:
                                           y + 2;
                    }
                    else if (x < 0)
                    {
                        int someVariable = (y +
                                            z);
                        someVariable = x =
                                       x +
                                       y;
                        
                        String string1 = "This is a long" +
                                         " string which contains x = " +
                                         x;
                    }
                    else
                    {
                        label2:
                        for (int i = 0; i < 5; i++)
                        {
                            doSomething(i);
                        }
                    }
                    switch (a)
                    {
                        case 0:
                            doCase0();
                            break;
                        default:
                            doDefault();
                    }
                }
                catch (Exception e)
                {
                    processException(e.getMessage(), x + y, z, a);
                }
                finally
                {
                    processFinally();
                }
            }
     
            if (2 < 3)
            {
                return;
            }
    
            if (3 < 4)
            {
                return;
            }
            else
            {
                break;
            }
     
            do
            {
                x++;
            }
            while (x < 10000);
            
            while (x > 0)
            {
                System.out.println(x--);
            }
    
            for (int i = 0; i < 5; i++)
            {
                System.out.println(i);
            }
            
            for (int i = 0; i < 5; i++)
            {
                if (i != 1)
                {
                    System.out.println(i);
                }
                else
                {
                    System.out.println("This is it.");
                }
            }
        }
        
        private class InnerClass
            implements I1,
                       I2
        {
            public void bar()
                    throws E1,
                           E2
            {
                System.out.println("bar");
            }
        }
        
    }
    ```

## Code style profiles

We have created code style profiles which you can import into your IDE:

* <a href="{{resources}}/codestyles/s3fs-idea.xml" target="_blank">s3fs-idea.xml</a>
* <a href="{{resources}}/codestyles/s3fs-eclipse.xml" target="_blank">s3fs-eclipse.xml</a>
